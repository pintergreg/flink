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

package org.apache.flink.streaming.api.ft;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.util.Collector;

/**
 * Test created ... just for fun ;)
 */
public class PrimeCount {

	public static void main(String[] args) throws Exception {

		primeCounting();
	}

	/* ACTUAL TEST-TOPOLOGY METHODS */

	private static void primeCounting() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// building the job graph
		/*
		*  (So)--(F)--(C)--(Si)
		*
		* Source emits numbers (Integer) from 0 to n, Filter lets pass prime numbers
		* count counts the prime numbers and Sink prints it to standard error output
		*/
		DataStream<Long> sourceStream1 = env.addSource(new NumberSource(50)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		sourceStream1.filter(new PrimeFilter()).count().setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).addSink(new LongSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//run this topology
		env.execute();
	}


	/* SOURCE CLASSES */

	private static final class NumberSource implements SourceFunction<Long> {
		private long n;
		Type type;

		public enum Type {
			ODD, EVEN, BOTH
		}

		public NumberSource(long n) {
			this.n = n;
			this.type = Type.BOTH;
		}

		public NumberSource(long n, Type type) {
			this.n = n;
			this.type = type;
		}

		@Override
		public void invoke(Collector<Long> collector) throws Exception {
			int step;
			int start;
			switch (this.type) {
				case EVEN:
					step = 2;
					start = 0;
					break;
				case ODD:
					step = 2;
					start = 1;
					break;
				case BOTH:
				default:
					step = 1;
					start = 0;
					break;
			}
			for (long i = start; i < n; i += step) {
				//Thread.sleep(105L); // forcing out the replay
				collector.collect(i);
			}
		}
	}

	/* SINK CLASSES */

	public static class LongSink implements SinkFunction<Long> {

		@Override
		public void invoke(Long value) {
			System.err.println(value);
		}
	}

	/* FILTER CLASSES */

	public static class PrimeFilter implements FilterFunction<Long> {

		@Override
		public boolean filter(Long value) throws Exception {
			boolean result = true;
			long n = Math.round(Math.sqrt(value))+1;
			for (long i = 2; i < n; i++) {
				if (value % i == 0) {
					result = false;
					break;
				}
			}
			return result && value > 1;
		}

	}

}
