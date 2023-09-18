package org.myorg.processing;

import java.lang.Exception;
import java.sql.Timestamp;
import com.google.gson.Gson;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class AccountEntry {
    public Timestamp timestamp;
    public Integer value;
    public String way;
    public String source;
    public String account;
    public String description;

    public AccountEntry(Timestamp timestamp, Integer value, String way,
        String source, String account, String description) {
        this.timestamp = timestamp;
        this.value = value;
        this.way = way;
        this.source = source;
        this.account = account;
        this.description = description;
    }

    public static KafkaSink<AccountEntry> getKafkaSink(String kafkaAddress, String outputTopic) {
        return KafkaSink.<AccountEntry>builder()
				.setBootstrapServers(kafkaAddress)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
					.setTopic(outputTopic)
					.setValueSerializationSchema(new AccountEntrySerializationSchema())
					.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
    }

    public static class AccountEntrySerializationSchema implements SerializationSchema<AccountEntry> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
        }

        @Override
        public byte[] serialize(AccountEntry entry) {
            Gson g = new Gson();
			return g.toJson(entry).getBytes();
        }
    }
}
