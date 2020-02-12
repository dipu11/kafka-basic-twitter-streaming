package demo;

import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Constants;

import java.util.Properties;

/**
 * Created by Dipu on 2/2/20.
 */
public class Producer {

    public static void  producerOne()
    {

        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producerOne= new KafkaProducer<String, String>(properties);

        User user= new User("Mr. karim", "25");
        ProducerRecord<String, String> record= new ProducerRecord<String, String>(Constants.TOPIC, user.toString());




        producerOne.send(record);
        producerOne.flush();
        producerOne.close();

        System.out.println("object sending done!");
    }

}
