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


package org.apache.flink.examples.java.multicast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.multicast.MulticastMessage;
import org.apache.flink.util.Collector;

public class MulticastChainedMapDriverTest {

	public static void main(String[] args) throws Exception {
//		final ExecutionEnvironment env = ExecutionEnvironment
//				.getExecutionEnvironment();

		String pathToJar="/home/fberes/sztaki/git/incubator-flink/flink-examples/flink-java-examples/target/original-flink-java-examples-0.8-incubating-SNAPSHOT.jar";
		final ExecutionEnvironment env = ExecutionEnvironment
				.createRemoteEnvironment("127.0.0.1",6123, pathToJar);
		
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Double, long[]>> data = env
				.fromElements(new Tuple3<Long, Double, long[]>(0L, 0.3,
						new long[] { 0L, 1L, 2L, 3L }),
						new Tuple3<Long, Double, long[]>(1L, 0.1, new long[] {
								0L, 2L }), new Tuple3<Long, Double, long[]>(2L,
								0.2, new long[] { 0L, 1L }));

		System.out.println("Original data:");
		data.print();

		DataSet<MulticastMessage> messages = data
				.flatMap(new FlatMapFunction<Tuple3<Long, Double, long[]>, MulticastMessage>() {
					@Override
					public void flatMap(Tuple3<Long, Double, long[]> value,
							Collector<MulticastMessage> out) throws Exception {
						out.collect(new MulticastMessage(value.f2, value.f1));
					}
				});
		// if this print is commented then only vertex 0 gets messages! WHY?
		//messages.print();

		// // cannot group by on array field!
		// messages.groupBy(0).sum(1).print();

		{
			// Chained version: if this map block is commented then
			// MulticastCollector has only 1 writer, otherwise 2.
			messages.map(
					new MapFunction<MulticastMessage, Tuple2<Long, Double>>() {
						@Override
						public Tuple2<Long, Double> map(MulticastMessage value)
								throws Exception {
							return new Tuple2<Long, Double>(value.f0[0],
									value.f1);
						}
					}).print();
		}

//		{
//			// Unchained version: if this map block is commented then
//			// MulticastCollector has only 1 writer, otherwise 2.
//			DataSet<Tuple2<Long, Double>> formattedMessages = messages
//					.map(new MapFunction<MulticastMessage, Tuple2<Long, Double>>() {
//						@Override
//						public Tuple2<Long, Double> map(MulticastMessage value)
//								throws Exception {
//							System.out.println(value);
//							return new Tuple2<Long, Double>(value.f0[0],
//									value.f1);
//						}
//					});
//			formattedMessages.print();
//		}

//		{
//			// Unchained version: with MulticastMessage: this way the
//			// MulticastCollector is invoked which is good!
//			DataSet<MulticastMessage> messages2 = messages
//					.map(new MapFunction<MulticastMessage, MulticastMessage>() {
//						@Override
//						public MulticastMessage map(MulticastMessage value)
//								throws Exception {
//							// System.out.println(value);
//							return new MulticastMessage(value.f0, value.f1 + 1);
//						}
//					});
//			//messages2.print();
//
//			// Chained version: if this map block is commented then
//			// MulticastCollector has only 1 writer, otherwise 2.
//			messages2.map(
//					new MapFunction<MulticastMessage, Tuple2<Long, Double>>() {
//						@Override
//						public Tuple2<Long, Double> map(MulticastMessage value)
//								throws Exception {
//							return new Tuple2<Long, Double>(value.f0[0],
//									value.f1);
//						}
//					}).print();
//		}

		env.setDegreeOfParallelism(1);
		env.execute("Multicast Test");
	}

}
