package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import utils.Constants;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by Dipu on 2/3/20.
 */
public class StreamAPI {

    public static void main(String args[]) throws InterruptedException {

        String inputTopic = Constants.TOPIC_STREAM;
        String filePath="/media/tigerit/projects/playground/files";
        File file=new File(filePath);

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "wordcount-stream-test");
        streamsConfiguration.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Constants.BOOTSTRAP_SERVER);

        streamsConfiguration.put(
                StreamsConfig.KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.STATE_DIR_CONFIG,
                file.getAbsolutePath());


        KStreamBuilder builder= new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(inputTopic);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

     /*   KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();*/
      //  Map<String, String> mp=textLines.flatMapValues(val -> (key, val));


        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Thread.sleep(3000);
        streams.close();
    }


}
