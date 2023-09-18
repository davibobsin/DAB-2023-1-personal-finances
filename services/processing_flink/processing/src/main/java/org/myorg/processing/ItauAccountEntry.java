package org.myorg.processing;

import java.io.IOException;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class ItauAccountEntry {
    public String dataLancamento;
    public String descricaoLancamento;
    public String descricaoDetalhadaLancamento;
    public String valorLancamento;
    public Boolean saldoDia;
    public String labelMes;
    public String descricaoMesAno;
    public Boolean ePositivo;
    public String indicadorOperacao;

    public static KafkaSource<ItauAccountEntry> getKafkaSource(String kafkaAddress, String inputTopic, String consumerGroup) {
        return KafkaSource.<ItauAccountEntry>builder()
            .setBootstrapServers(kafkaAddress)
            .setProperty("enable.auto.commit", "true")
            .setProperty("auto.commit.interval.ms", "500")
            .setProperty("partition.discovery.interval.ms", "10000")
            .setTopics(inputTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new ItauAccountEntryDeserializationSchema())
            .build();
    }

    public static class ItauAccountEntryDeserializationSchema extends AbstractDeserializationSchema<ItauAccountEntry> {
        @Override
        public ItauAccountEntry deserialize(byte[] rawMessage) throws IOException {
            Gson g = new Gson();
            String jsonString = new String(rawMessage, StandardCharsets.UTF_8);
            return g.fromJson(jsonString, ItauAccountEntry.class);
        }
    }
}
