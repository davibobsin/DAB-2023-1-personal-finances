package org.myorg.processing;

import java.io.IOException;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class NubankCardEntry {
    public class NubankCardEntryDetails {
        public String status;
        public Double lat;
        public Double lon;
        public String subcategory;
    }
    public NubankCardEntryDetails details;
    public Timestamp time;
    public String title;
    public Integer amount;
    public String description;
    public String account;

    public static KafkaSource<NubankCardEntry> getKafkaSource(String kafkaAddress, String inputTopic, String consumerGroup) {
        return KafkaSource.<NubankCardEntry>builder()
            .setBootstrapServers(kafkaAddress)
            .setProperty("enable.auto.commit", "true")
            .setProperty("auto.commit.interval.ms", "500")
            .setProperty("partition.discovery.interval.ms", "10000")
            .setTopics(inputTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new NubankCardEntryDeserializationSchema())
            .build();
    }

    public static class NubankCardEntryDeserializationSchema extends AbstractDeserializationSchema<NubankCardEntry> {
        @Override
        public NubankCardEntry deserialize(byte[] rawMessage) throws IOException {
            Gson g = new Gson();
            String jsonString = new String(rawMessage, StandardCharsets.UTF_8);
            return g.fromJson(jsonString, NubankCardEntry.class);
        }
    }
}
