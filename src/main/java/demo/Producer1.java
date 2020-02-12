package demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * Created by Dipu on 2/2/20.
 */
public class Producer1 {

    private final static Logger logger= LoggerFactory.getLogger(Producer1.class);

    public static KafkaProducer<String, String> getProducer()
    {
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producerOne= new KafkaProducer<String, String>(properties);

        return  producerOne;
    }

    /**
     * producer with callback onCompletion(){...}
     */
    public static void simpleDataProductionWithMeta()
    {

        KafkaProducer<String, String> producerOne= getProducer();


        ProducerRecord<String, String> record= new ProducerRecord<String, String>("topic_3", "Hey dude...12345...!");


        producerOne.send(record, new Callback()
        {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e== null) {
                    System.out.println("Successfully received the details as: \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset() + "\n" +
                            "Timestamp" + recordMetadata.timestamp());
                }

                else {
                    System.out.println("Can't produce,getting error"+e);

                }

            }
        });
        producerOne.flush();
        producerOne.close();

    }

    public static void produceDataWithMetaToPartitions () throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producerOne= getProducer();

        for(int i=0;i<15;i++)
        {
            String topic=Constants.TOPIC_STREAM;
            String value="vlaue -> :: one_"+Integer.toString(i);
            String key="id_"+ Integer.toString(i);

            Thread.sleep(5000);
            System.out.println("key:"+ key);

            ProducerRecord<String, String> record= new ProducerRecord<String, String>(topic, key, value);
            producerOne.send(record, new Callback()
            {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e== null) {
                        System.out.println();
                        System.out.println("Successfully received the details as: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    }

                    else {
                        System.out.println("Can't produce,getting error"+e);

                    }

                }
            }).get();
        }

        producerOne.flush();
        producerOne.close();

    }


    public static void main(String args[]) throws ExecutionException, InterruptedException {
        System.out.println("Hi, there...from Producer 1");
        produceDataWithMetaToPartitions();

        //objectProducer();

    }


}
