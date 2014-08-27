/**
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

package org.apache.flink.streaming.examples.latency;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class LatencyJob {

	public static class ForLatencySource implements SourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		private boolean firstRun;
		
		public ForLatencySource(boolean firstRun) {
			this.firstRun = firstRun;
		}

		@Override
		public void invoke(Collector<Long> collector) throws Exception {
			for (int i = 0; i < 10000000; i++) {
				if (firstRun) {
					collector.collect(System.currentTimeMillis());
					Thread.sleep(2000);
					firstRun = false;
				} else {
					collector.collect(System.currentTimeMillis());
				}
			}
		}
	}

	public static class WhileLatencySource implements SourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		private boolean firstRun = false;

		public WhileLatencySource(boolean firstRun) {
			this.firstRun = firstRun;
		}
		
		@Override
		public void invoke(Collector<Long> collector) throws Exception {
			while (true) {
				if (firstRun) {
					collector.collect(System.currentTimeMillis());
					Thread.sleep(2000);
					firstRun = false;
				} else {
					collector.collect(System.currentTimeMillis());
				}
			}
		}
	}

	public static class LatencyMap implements MapFunction<Long, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> map(Long value) throws Exception {
			return new Tuple2<Long, Long>(value, System.currentTimeMillis());
		}

	}

	public static class LatencySink implements SinkFunction<Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		long sum0 = 0;
		long sum1 = 0;
		long cnt = 0;
		
		@Override
		public void invoke(Tuple2<Long, Long> value) {
			sum0 += value.f1 - value.f0;
			sum1 += System.currentTimeMillis() - value.f1;
			cnt++;
			
			if (cnt % 1000 == 0){
				System.out.println(((float) sum0 / cnt) + ", \t" + ((float) sum1 / cnt));
				sum0 = 0;
				sum1 = 0;
				cnt = 0;
			}
			
		}

	}

	public static class DummyLatencySink implements SinkFunction<Long> {
		private static final long serialVersionUID = 1L;
		
		long sum = 0;
		long cnt = 0;

		@Override
		public void invoke(Long value) {
			sum += System.currentTimeMillis() - value;
			cnt++;
			
			if (cnt % 10000 == 0){
				System.out.println((float) sum / cnt);
				sum = 0;
				cnt = 0;
			}

		}

	}

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		env.setBufferTimeout(100);

//		DataStream<Long> dataStream = env.addSource(new ForLatencySource(false)).addSink(
//				new DummyLatencySink());
		DataStream<Tuple2<Long, Long>> dataStream = env.addSource(new ForLatencySource(true))
				.map(new LatencyMap())
				.addSink(new LatencySink());

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
