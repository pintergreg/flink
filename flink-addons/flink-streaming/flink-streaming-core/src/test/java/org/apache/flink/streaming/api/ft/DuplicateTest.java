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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Test created for testing edge information gathering and replaypartition setting
 */
public class DuplicateTest {

	public static void main(String[] args) throws Exception {

		numberSequenceWithoutShuffle();
	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */


	private static void numberSequenceWithoutShuffle() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// building the job graph
		/*
		*  (So)--(M)--(Si)
		*
		* Source emits numbers as String from 0 to 9
		* Filter does nothing, lets pass everything
		* Sink prints values to standard error output
		*/
		DataStream<Integer> sourceStream1 = env.addSource(new NumberSource(10)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		sourceStream1.map(new NumberMap()).addSink(new SimpleSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//run this topology
		env.execute();
	}



	/*
	 * SOURCE CLASSES
	 */

	private static final class NumberSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private int n;
		Type type;

		public enum Type {
			ODD, EVEN, BOTH
		}

		public NumberSource(int n) {
			this.n = n;
			this.type = Type.BOTH;
		}

		public NumberSource(int n, Type type) {
			this.n = n;
			this.type = type;
		}

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {
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
			for (int i = start; i < n; i += step) {
				collector.collect(i);
			}
		}
	}

	/*
	 * MAP CLASSES
	 */

	public static class NumberMap implements MapFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		private ArrayList<Long> recordsToFail;


		public NumberMap() {
			this.recordsToFail = new ArrayList<Long>();
		}

		@Override
		public String map(Integer value) throws Exception {
			return value.toString();
		}
	}

	/*
	 * SINK CLASSES
	 */

	public static class SimpleSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;
		private ArrayList<Long> recordsToFail;

		public SimpleSink() {
			this.recordsToFail = new ArrayList<Long>();
		}

		@Override
		public void invoke(String value) {
			System.err.println(value);
		}
	}

}
