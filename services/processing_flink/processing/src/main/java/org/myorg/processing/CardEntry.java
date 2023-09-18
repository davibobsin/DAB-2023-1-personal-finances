package org.myorg.processing;

import java.lang.Exception;
import java.sql.Timestamp;
import com.google.gson.Gson;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class CardEntry {
    public Timestamp timestamp;
    public Integer value;
    public String label;
    public String description;

    public CardEntry(Timestamp timestamp, Integer value, String label, String description) {
        this.timestamp = timestamp;
        this.value = value;
        this.label = label;
        this.description = description;
    }

    public static KafkaSink<CardEntry> getKafkaSink(String kafkaAddress, String outputTopic) {
        return KafkaSink.<CardEntry>builder()
				.setBootstrapServers(kafkaAddress)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
					.setTopic(outputTopic)
					.setValueSerializationSchema(new CardEntrySerializationSchema())
					.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
    }

    public static class CardEntrySerializationSchema implements SerializationSchema<CardEntry> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
        }

        @Override
        public byte[] serialize(CardEntry entry) {
            Gson g = new Gson();
			return g.toJson(entry).getBytes();
        }
    }
}
