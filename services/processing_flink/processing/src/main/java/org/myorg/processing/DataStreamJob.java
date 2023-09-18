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

package org.myorg.processing;

import java.util.Arrays;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.myorg.processing.Util;
import org.myorg.processing.AccountEntry;
import org.myorg.processing.CardEntry;
import org.myorg.processing.NubankCardEntry;
import org.myorg.processing.NubankAccountEntry;
import org.myorg.processing.ItauAccountEntry;

// For debug only
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		final String KAFKA_ADDRESS = "redpanda-0:9092";
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//-----------------------------------------
		// Transformations
		//-----------------------------------------
		env.fromSource(NubankCardEntry.getKafkaSource(KAFKA_ADDRESS, "nubank.card.entry", "consumer_card_nubank"), WatermarkStrategy.noWatermarks(), "Kafka Source")
		   .flatMap(new ProcessNubankCardEntry())
		   .sinkTo(CardEntry.getKafkaSink(KAFKA_ADDRESS, "processed.card.entry"))
		   .name("Kafka - Card Entry");

		env.fromSource(NubankAccountEntry.getKafkaSource(KAFKA_ADDRESS, "nubank.account.entry", "consumer_account_nubank"), WatermarkStrategy.noWatermarks(), "Kafka Source")
		   .flatMap(new ProcessNubankAccountEntry())
		   .sinkTo(AccountEntry.getKafkaSink(KAFKA_ADDRESS, "processed.account.entry"))
		   .name("Kafka - Account Entry");

		env.fromSource(ItauAccountEntry.getKafkaSource(KAFKA_ADDRESS, "itau.account.entry", "consumer_account_itau"), WatermarkStrategy.noWatermarks(), "Kafka Source")
		   .flatMap(new ProcessItauAccountEntry())
		   .sinkTo(AccountEntry.getKafkaSink(KAFKA_ADDRESS, "processed.account.entry"))
		   .name("Kafka - Account Entry");

		//-----------------------------------------
		// Fraud Detection
		//-----------------------------------------
		env.fromSource(NubankCardEntry.getKafkaSource(KAFKA_ADDRESS, "nubank.card.entry", "consumer_card_nubank"), WatermarkStrategy.noWatermarks(), "Kafka Source")
		   .keyBy(value -> value.account)
		   .flatMap(new GeolocationSpeedFraudDetection())
		   .sinkTo(Fraud.getKafkaSink(KAFKA_ADDRESS, "processed.fraud"))
		   .name("Kafka - Fraud");

		env.execute("Processing Data");
	}

	public static class ProcessNubankCardEntry implements FlatMapFunction<NubankCardEntry, CardEntry> {
		@Override
		public void flatMap(NubankCardEntry entry, Collector<CardEntry> out) {
			out.collect(new CardEntry(entry.time, entry.amount, entry.title, entry.description));
		}
	}

	public static class ProcessNubankAccountEntry implements FlatMapFunction<NubankAccountEntry, AccountEntry> {
		@Override
		public void flatMap(NubankAccountEntry entry, Collector<AccountEntry> out) {
			if (entry.strikethrough) {
				return;
			}
			Integer amount = Math.round(entry.amount * 100);
			String way = "OUT";
			if (Arrays.asList(entry.tags).contains("money-in")) {
				way = "IN";
			}
			out.collect(new AccountEntry(
				Util.convertStringToTimestamp("yyyy-MM-dd", entry.postDate),
				amount,
				way,
				entry.title,
				"nubank",
				String.format("(%s) %s", entry.footer, entry.detail)
			));
		}
	}

	public static class ProcessItauAccountEntry implements FlatMapFunction<ItauAccountEntry, AccountEntry> {
		@Override
		public void flatMap(ItauAccountEntry entry, Collector<AccountEntry> out) {
			Boolean isLabelEntry = (entry.saldoDia || (entry.labelMes != null) || (entry.descricaoMesAno != null));
			if (isLabelEntry) {
				return;
			}
			Float amountFloat = Float.parseFloat(entry.valorLancamento.replace(".", "").replace(",", "."));
			Integer amount = Math.round(amountFloat * 100);
			String way = "OUT";
			if (entry.ePositivo) {
				way = "IN";
			}

			out.collect(new AccountEntry(
				Util.convertStringToTimestamp("dd/MM/yyyy", entry.dataLancamento),
				amount,
				way,
				entry.descricaoLancamento,
				"itau",
				String.format("(%s) %s", entry.indicadorOperacao, entry.descricaoDetalhadaLancamento)
			));
		}
	}

	public static class GeolocationSpeedFraudDetection extends RichFlatMapFunction<NubankCardEntry, Fraud> {
		private static final Logger LOG = LoggerFactory.getLogger(GeolocationSpeedFraudDetection.class);
		private transient ValueState<NubankCardEntry> previous;
		private final Integer MAX_SPEED_KM_PER_HOUR = 600;

		@Override
		public void flatMap(NubankCardEntry input, Collector<Fraud> out) throws Exception {
			if (!input.details.subcategory.equals("card_present")) {
				return;
			}

			NubankCardEntry previousEntry = previous.value();
			if (previousEntry == null) {
				previous.update(input);
				return;
			}
			double distanceMeters = Util.HaversineDistance(
				previousEntry.details.lat,
				input.details.lat,
				previousEntry.details.lon,
				input.details.lon
			);

			int deltaSeconds = Util.TimestampDiffInSeconds(previousEntry.time, input.time);

			if ((deltaSeconds == 0) || (distanceMeters == 0)) {
				return;
			}

			int speedKhH = (((int)distanceMeters/1000)/(deltaSeconds/3600));
			if (speedKhH > MAX_SPEED_KM_PER_HOUR) {
				String[] strArray = new String[2];
				strArray[0] = String.format("(%s) %s", input.title, input.description);
				strArray[1] = String.format("(%s) %s", previousEntry.title, previousEntry.description);
				out.collect(new Fraud("speed_geolocation", strArray));
			}

			previous.update(input);
		}

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<NubankCardEntry> descriptor =
					new ValueStateDescriptor<NubankCardEntry>(
							"stream_nubank_card_entry",
							TypeInformation.of(new TypeHint<NubankCardEntry>() {}),
							null);
			previous = getRuntimeContext().getState(descriptor);
		}
	}
}


