package org.myorg.postgresql_sink;

import java.io.IOException;
import java.time.ZoneOffset;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import org.postgresql.Driver;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class Error {
    public Timestamp timestamp;
    public String step;
    public String description;
    public String metadata;

    public static KafkaSource<Error> getKafkaSource(String kafkaAddress, String inputTopic, String consumerGroup) {
        return KafkaSource.<Error>builder()
            .setBootstrapServers(kafkaAddress)
            .setProperty("enable.auto.commit", "true")
            .setProperty("auto.commit.interval.ms", "500")
            .setProperty("partition.discovery.interval.ms", "10000")
            .setTopics(inputTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new ErrorDeserializationSchema())
            .build();
    }

    public static SinkFunction<Error> getPostgresSink(String url, String user, String password) {
        return JdbcSink.sink(
            "insert into public.errors (timestamp, step, description, metadata) values (?, ?, ?, ?)",
            (statement, entry) -> {
                statement.setTimestamp(1, entry.timestamp);
                statement.setString(2, entry.step);
                statement.setString(3, entry.description);
                statement.setString(4, entry.metadata);
            },
            JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(url)
                    .withDriverName("org.postgresql.Driver")
                    .withUsername(user)
                    .withPassword(password)
                    .build()
        );
    }

    public static class ErrorDeserializationSchema extends AbstractDeserializationSchema<Error> {
        @Override
        public Error deserialize(byte[] rawMessage) throws IOException {
            Gson g = new Gson();
            String jsonString = new String(rawMessage, StandardCharsets.UTF_8);
            return g.fromJson(jsonString, Error.class);
        }
    }
}
