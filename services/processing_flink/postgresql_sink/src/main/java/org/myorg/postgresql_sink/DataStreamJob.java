/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.myorg.postgresql_sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.myorg.postgresql_sink.AccountEntry;
import org.myorg.postgresql_sink.CardEntry;
import org.myorg.postgresql_sink.Error;
import org.myorg.postgresql_sink.Fraud;

public class DataStreamJob {
	final static String CONSUMER_GROUP = "consumerSinkGroup";
	final static String KAFKA_ADDRESS = "redpanda-0:9092";

	final static String POSTGRES_URL = "jdbc:postgresql://postgres:5432/personal_finances";
	final static String POSTGRES_USER = "user";
	final static String POSTGRES_PASSWORD = "password";

	public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
            3, // max failures per interval
            Time.minutes(5), //time interval for measuring failure rate
            Time.seconds(10) // delay
        ));

        env.fromSource(AccountEntry.getKafkaSource(KAFKA_ADDRESS, "processed.account.entry", "pg_consumer_account_entries"), WatermarkStrategy.noWatermarks(), "Kafka")
            .addSink(AccountEntry.getPostgresSink(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))
            .name("PostgreSQL [account_entries]");

        env.fromSource(CardEntry.getKafkaSource(KAFKA_ADDRESS, "processed.card.entry", "pg_consumer_card_entries"), WatermarkStrategy.noWatermarks(), "Kafka")
            .addSink(CardEntry.getPostgresSink(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))
            .name("PostgreSQL [card_entries]");

        env.fromSource(Error.getKafkaSource(KAFKA_ADDRESS, "processed.error", "pg_consumer_errors"), WatermarkStrategy.noWatermarks(), "Kafka")
            .addSink(Error.getPostgresSink(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))
            .name("PostgreSQL [errors]");

        env.fromSource(Fraud.getKafkaSource(KAFKA_ADDRESS, "processed.fraud", "pg_consumer_frauds"), WatermarkStrategy.noWatermarks(), "Kafka")
            .addSink(Fraud.getPostgresSink(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))
            .name("PostgreSQL [frauds]");

		env.execute("PostgreSQL Sink");
	}
}
