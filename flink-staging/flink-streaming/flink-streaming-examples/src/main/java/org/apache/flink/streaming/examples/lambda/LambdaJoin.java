/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.lambda;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction.WatchType;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class LambdaJoin {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Tuple2<String, Tuple2<String, Integer>>> dataSetStream = streamEnv
				.readFileStream("/Users/gyfora/FlinkTmp", 1000, WatchType.PROCESS_ONLY_APPENDED)
				.map(new Parser());

		dataSetStream.print();

		DataStream<Tuple2<String, Integer>> myStream = streamEnv.addSource(new MySource());

		dataSetStream.connect(myStream).flatMap(new Join()).print();

		streamEnv.execute();
	}

	private static class MySource implements SourceFunction<Tuple2<String, Integer>> {

		Random rnd = new Random();
		String[] words = { "first", "second", "third" };

		@Override
		public void invoke(Collector<Tuple2<String, Integer>> collector) throws Exception {
			for (int i = 0; i < 100; i++) {
				Thread.sleep(2000);
				collector.collect(new Tuple2<String, Integer>(words[rnd.nextInt(words.length)], rnd
						.nextInt(100)));
			}
		}
	}

	private static class Join
			implements
			CoFlatMapFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> {

		Set<Tuple2<String, Integer>> dataSet = new HashSet<Tuple2<String, Integer>>();
		String currentSet = "";

		@Override
		public void flatMap1(Tuple2<String, Tuple2<String, Integer>> value,
				Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
			if (!currentSet.equals(value.f0)) {
				dataSet.clear();
				currentSet = value.f0;

			}
			dataSet.add(value.f1);
		}

		@Override
		public void flatMap2(Tuple2<String, Integer> value,
				Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
			for (Tuple2<String, Integer> element : dataSet) {
				if (element.f0.equals(value.f0)) {
					out.collect(new Tuple3<String, Integer, Integer>(element.f0, value.f1,
							element.f1));
				}
			}

		}

	}

	private static class Parser implements
			MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<String, Integer>>> {

		@Override
		public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, String> value)
				throws Exception {

			String[] split = value.f1.split(",");

			return new Tuple2<String, Tuple2<String, Integer>>(value.f0,
					new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1])));
		}

	}
}
