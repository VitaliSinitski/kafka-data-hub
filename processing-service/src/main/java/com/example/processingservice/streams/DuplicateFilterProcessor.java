package com.example.processingservice.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@EnableKafkaStreams
public class DuplicateFilterProcessor {

    // Определение названий входного и выходного топиков
    private static final String INPUT_TOPIC = "docs.article";
    private static final String OUTPUT_TOPIC = "filtered.docs.article";

    // Метод, слушающий выходной топик с фильтрованными сообщениями
    @KafkaListener(id = "filter-duplicates-consumer", topics = OUTPUT_TOPIC, groupId = "filter-duplicates-group")
    public void listenToFilteredTopic(String message) {
        // Обрабатывать отфильтрованные сообщения по необходимости
    }

    // Конструктор класса
    @SuppressWarnings("unchecked")
    public DuplicateFilterProcessor() {
        // Настройки Kafka Streams
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "duplicate-filter-processor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Создание StreamsBuilder для конструирования потоковой обработки
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Чтение сообщений из входного топика в потоке
        KStream<String, String> input = streamsBuilder.stream(INPUT_TOPIC);

        // Группировка по значению (содержимому сообщения) и подсчет количества
        KTable<String, Long> deduplicatedCounts = input
                .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        // Фильтрация сообщений с количеством равным 1 и запись в выходной топик
        deduplicatedCounts.toStream().filter((key, count) -> count == 1).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // Создание и запуск Kafka Streams приложения
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

        // Добавление shutdown hook для корректного завершения Kafka Streams приложения
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    /*
     * Этот класс реализует Kafka Streams процессор, который фильтрует дубликаты сообщений,
     * читая их из входного Kafka топика "docs.article"
     * и записывая неповторяющиеся сообщения в выходной топик "filtered.docs.article".
     *
     * Метод listenToFilteredTopic предназначен для обработки отфильтрованных сообщений.
     *
     * Конструктор класса инициализирует настройки Kafka Streams, создает потоки обработки,
     * группирует и фильтрует сообщения, а также запускает Kafka Streams приложение.
     *
     * Shutdown hook гарантирует правильное закрытие приложения при завершении.
     */
}
