package org.myorg.processing;

import java.io.IOException;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class NubankAccountEntry {
    public String[] tags;
    public String footer;
    public String title;
    public String detail;
    public String postDate;
    public Float amount;
    public Boolean strikethrough;

    public static KafkaSource<NubankAccountEntry> getKafkaSource(String kafkaAddress, String inputTopic, String consumerGroup) {
        return KafkaSource.<NubankAccountEntry>builder()
            .setBootstrapServers(kafkaAddress)
            .setProperty("enable.auto.commit", "true")
            .setProperty("auto.commit.interval.ms", "500")
            .setProperty("partition.discovery.interval.ms", "10000")
            .setTopics(inputTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new NubankAccountEntryDeserializationSchema())
            .build();
    }

    public static class NubankAccountEntryDeserializationSchema extends AbstractDeserializationSchema<NubankAccountEntry> {
        @Override
        public NubankAccountEntry deserialize(byte[] rawMessage) throws IOException {
            Gson g = new Gson();
            String jsonString = new String(rawMessage, StandardCharsets.UTF_8);
            return g.fromJson(jsonString, NubankAccountEntry.class);
        }
    }
}
