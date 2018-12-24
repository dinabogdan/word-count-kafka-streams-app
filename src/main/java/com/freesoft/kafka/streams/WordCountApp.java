package com.freesoft.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {
        Properties config = setUpKafkaConfigurations();
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String, String> wordCountInput = kStreamBuilder.stream("word-count-input");
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(String::toLowerCase)
                .flatMapValues(lowerCaseText -> Arrays.asList(lowerCaseText.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count("Counts");

        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, config);
        streams.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties setUpKafkaConfigurations() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.207.43:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }
}
