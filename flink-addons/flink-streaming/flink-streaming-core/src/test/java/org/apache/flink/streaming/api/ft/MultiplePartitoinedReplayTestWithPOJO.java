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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Test created for testing edge information gathering and replaypartition setting with POJOs
 */
public class MultiplePartitoinedReplayTestWithPOJO {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		*  (S1)
		*      \
		*       (M)--(Si)
		*      /
		*  (S2)
		* Source1 emits intPOJOs containing even number from 0 to 10
		* Source2 emits strPOJOs containing odd numbers from 0 to 10
		* Merge the two source and map integer to string
		* Sink prints values to standard error output
		*/
		DataStream<simpleIntegerPOJO> source1 = env.addSource(new intPOJOSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		DataStream<simpleStringPOJO> source2 = env.addSource(new strPOJOSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);



		source1.shuffle().merge(source2.map(new crossPOJOMap())).map(new integerPOJOMap()).addSink(new SimpleSink());


		//run this topology
		env.execute();
	}


	private static final class EmptyFilter implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return true;
		}
	}


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


	/*
	 * POJO tests with keyselector
	 */
	public static class simpleIntegerPOJO {
		private int number;

		public simpleIntegerPOJO() {
		}

		public simpleIntegerPOJO(int number) {
			this.number = number;
		}

		public int getNumber() {
			return number;
		}

		public void setNumber(int number) {
			this.number = number;
		}
	}

	public static class simpleStringPOJO {
		private String number;

		public simpleStringPOJO() {
		}

		public simpleStringPOJO(String number) {
			this.number = number;
		}

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}
	}

	public static final class intPOJOMap implements FlatMapFunction<simpleIntegerPOJO, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(simpleIntegerPOJO value, Collector<String> out) {
			out.collect(String.valueOf(value.getNumber()));
		}
	}

	public static final class integerPOJOMap implements MapFunction<simpleIntegerPOJO, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(simpleIntegerPOJO value) {
			return String.valueOf(value.getNumber());
		}
	}

	public static final class strPOJOMap implements FlatMapFunction<simpleStringPOJO, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(simpleStringPOJO value, Collector<String> out) {
			out.collect(value.getNumber());
		}
	}

	public static final class crossPOJOMap implements MapFunction<simpleStringPOJO, simpleIntegerPOJO> {
		private static final long serialVersionUID = 1L;

		@Override
		public simpleIntegerPOJO map(simpleStringPOJO value) throws Exception {
			return (new simpleIntegerPOJO(Integer.valueOf(value.getNumber())));
		}
	}

	private static final class intPOJOSource implements SourceFunction<simpleIntegerPOJO> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<simpleIntegerPOJO> collector) throws Exception {
			for (int i = 0; i < 10; i+=2) {
				collector.collect(new simpleIntegerPOJO(i));
			}
		}
	}

	private static final class strPOJOSource implements SourceFunction<simpleStringPOJO> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<simpleStringPOJO> collector) throws Exception {
			for (int i = 1; i < 10; i+=2) {
				collector.collect(new simpleStringPOJO(String.valueOf(i)));
			}
		}
	}


}
