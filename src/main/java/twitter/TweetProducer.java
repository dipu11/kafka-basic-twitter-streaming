package twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import demo.Producer;
import demo.Producer1;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dipu on 2/12/20.
 */

@Getter
@Setter
public class TweetProducer {


    public static String consumerKey;
    public static String consumerSecret;
    public static  String token;
    public static String secret;

    private final static Logger logger = LoggerFactory.getLogger(TweetProducer.class);



    public TweetProducer() {

        try (InputStream input = TweetProducer.class.getClassLoader().getResourceAsStream("twitterConfig.properties")) {

            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find twitterConfig.properties");
                return;
            }

            //load a properties file from class path, inside static method
            prop.load(input);


            this.consumerKey=prop.getProperty("app.twitter.consumerKey");
            this.consumerSecret=prop.getProperty("app.twitter.consumerSecret");
            this.token=prop.getProperty("app.twitter.token");
            this.secret=prop.getProperty("app.twitter.secret");



        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void runApp()
    {
        /**
         *  Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
         **/

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
        KafkaProducer<String, String> producer= getProducer();
        Client client=getTwitterClient(msgQueue);
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);//specify the time
                System.out.println("polledMessage: "+ msg);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                System.out.println(msg);
                producer.send(new ProducerRecord<String, String>("twitter_topic", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            System.out.println("Something went wrong"+e);
                        }
                        else
                        {
                            System.out.println("Successfully received the details as: \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Offset" + recordMetadata.offset() + "\n" +
                                    "Timestamp" + recordMetadata.timestamp());
                        }
                    }
                });
            }

        }

        client.stop();

        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());

    }

    public static Client getTwitterClient(BlockingQueue<String> msgQueue)
    {

       /**
        * Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
       // System.out.println("consumerKey:"+ consumerKey+" , consumerSecret:"+ consumerSecret);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
             //   .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        // Attempts to establish a connection.
        //hosebirdClient.connect();
    }

    private static KafkaProducer<String, String> getProducer()
    {
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, utils.Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producerOne= new KafkaProducer<String, String>(properties);

        return  producerOne;
    }

    public static void main(String[] args) {
        new TweetProducer().runApp();
    }

}
