package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**
 * Created by Dipu on 2/3/20.
 */
public class Consumer {

    private final static Logger logger= LoggerFactory.getLogger(Consumer.class);


    public static KafkaConsumer<String, String> getConsumer()
    {
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUPD_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_MECHANISM);

        KafkaConsumer<String, String> consumerOne=new KafkaConsumer<String, String>(properties);

        return consumerOne;

    }

    public static void pollingData()
    {
        KafkaConsumer<String, String> consumerOne= getConsumer();
        consumerOne.subscribe(Arrays.asList(Constants.TOPIC));

        while(true)
        {
            ConsumerRecords<String, String> records= consumerOne.poll(Duration.ofMillis(5000));
            System.out.println(".......");

            for(ConsumerRecord<String, String> record: records)
            {
                System.out.println();
                System.out.println("key:"+ record.key()+",\nvalue:"+ record.value());
                System.out.println("partition:"+ record.partition()+", offset:"+ record.offset());
            }
        }
    }

    public static void main(String args[])
    {
        System.out.println("Hi, from consumer main...");
        pollingData();
    }
}
