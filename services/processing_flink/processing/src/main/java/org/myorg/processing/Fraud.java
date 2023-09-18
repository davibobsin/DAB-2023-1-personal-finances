package org.myorg.processing;

import java.lang.Exception;
import java.sql.Timestamp;
import com.google.gson.Gson;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class Fraud {
    public String policy;
    public String[] source_events;

    public Fraud(String policy, String[] source_events) {
        this.policy = policy;
        this.source_events = source_events;
    }

    public static KafkaSink<Fraud> getKafkaSink(String kafkaAddress, String outputTopic) {
        return KafkaSink.<Fraud>builder()
				.setBootstrapServers(kafkaAddress)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
					.setTopic(outputTopic)
					.setValueSerializationSchema(new FraudSerializationSchema())
					.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
    }

    public static class FraudSerializationSchema implements SerializationSchema<Fraud> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
        }

        @Override
        public byte[] serialize(Fraud entry) {
            Gson g = new Gson();
			return g.toJson(entry).getBytes();
        }
    }
}
