package br.com.rafaellino;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10); // fast processing

        StreamsBuilder kbuilder = new StreamsBuilder();
        // kbuilder.topology.describe() to get topology description
        KStream<String, String> wordCount = kbuilder.stream("word-count-input");

        KTable<String, Long> wordCountsStream = wordCount
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key, value) -> value) // potential repartitioning
                .groupByKey() // Group the records by key (potential repartitioning)
                .count(Named.as("Count")); // Perform a count operation on each key (aggregation)

        Produced<String, Long> produced = Produced.with(Serdes.String(), Serdes.Long());
        wordCountsStream.toStream().to("word-count-output", produced);

        KafkaStreams streams = new KafkaStreams(kbuilder.build(), properties);
        streams.start();
        System.out.println(streams.toString()); // print topology

        // graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}