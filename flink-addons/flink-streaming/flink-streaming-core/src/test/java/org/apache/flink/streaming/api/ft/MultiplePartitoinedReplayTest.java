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
public class MultiplePartitoinedReplayTest {

	public static void main(String[] args) throws Exception {

		evenOddNumberSequence(10);

	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */

	private static void numberSequence() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// building the job graph
		/*      (F)
		*      /   \
		*  (So)    (Si)
		*      \   /
		*       (M)
		* Source emits numbers as String from 0 to 9
		* Filter does nothing, lets pass everything
		* Sink prints values to standard error output
		*/
		DataStream<String> sourceStream1 = env.addSource(new SimpleSource());
		sourceStream1.shuffle().filter(new EmptyFilter())
				.merge(sourceStream1.map(new SimpleMap()))
				.addSink(new SimpleSink());

		//run this topology
		env.execute();
	}

	private static void evenOddNumberSequence() throws Exception {
		evenOddNumberSequence(10);
	}
	private static void evenOddNumberSequence(int n) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		*  (S1)
		*      \
		*       (M)--(Si)
		*      /
		*  (S2)
		* Source1 emits even numbers from 0 to 10
		* Source2 emits odd numbers from 0 to 10
		* Merge the two source and map integer to string
		* Sink prints values to standard error output
		*/
		DataStream<Integer> source1 = env.addSource(new NumberSource(n, NumberSource.Type.EVEN)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		DataStream<Integer> source2 = env.addSource(new NumberSource(n, NumberSource.Type.ODD)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//source1.addSink(new NumberSink());
		//source2 emits numbers, map them to string and sink strings...
		//source2.map(new NumberMap()).addSink(new SimpleSink());

		//source1.merge(source2).map(new NumberMap()).addSink(new SimpleSink());

		//different partitioning in src1 (shuffle) and src2 (default, forward)
		source1.shuffle().merge(source2).map(new NumberMap()).addSink(new SimpleSink());

		//run this topology
		env.execute();
	}

	/*
	 * SOURCE CLASSES
	 */

	private static final class SimpleSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<String> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				collector.collect("" + i);
			}
		}
	}

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

	public static class SimpleMap implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		private ArrayList<Long> recordsToFail;


		public SimpleMap() {
			this.recordsToFail = new ArrayList<Long>();
		}

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

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
	 * FILTER CLASSES
	 */

	private static final class EmptyFilter implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return true;
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

	public static class NumberSink implements SinkFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private ArrayList<Long> recordsToFail;

		public NumberSink() {
			this.recordsToFail = new ArrayList<Long>();
		}

		@Override
		public void invoke(Integer value) {
			System.err.println(value);
		}
	}

}
