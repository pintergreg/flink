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
import org.apache.flink.streaming.util.ExactlyOnceParameters;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Test created for testing edge information gathering and replaypartition setting
 */
public class ComplexTest {

	public static void main(String[] args) throws Exception {

		numberSequenceWithoutShuffle();
	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */

	private static void numberSequenceWithoutShuffle() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);
		env.setExactlyOnceExecution(new ExactlyOnceParameters(1000000, 0.000001, 5000));
		// building the job graph
		/*
		*  (So1)--(M1)--(M2)
		*                   \
		*                    (Merge)--(Si)
		*                   /
		*  (So2)--------(M3)
		*
		* Source1 emits three numbers in a POJO
		* Source2 emits two numbers in a POJO
		* Map1 calculates two number from the ingress three ones and emits a POJO
		* Map2 and Map3 calculates a number from the ingress two ones and emits an Integer
		* Sink prints values to standard error output
		*/
		DataStream<ThreeNumbers> sourceStream1 = env.addSource(new ThreeSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		DataStream<TwoNumbers> sourceStream2 = env.addSource(new TwoSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		sourceStream1.map(new ThreeMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).map(new TwoMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.merge(sourceStream2.map(new TwoMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)).map(new EmptyMap()).addSink(new NumberSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//sourceStream1.map(new ThreeMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).map(new TwoMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).addSink(new NumberSink());
		//		sourceStream2.map(new TwoMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).addSink(new NumberSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//run this topology
		env.execute();
	}


	/*
	 * SOURCE CLASSES
	 */

	private static final class ThreeSource implements SourceFunction<ThreeNumbers> {
		private static final long serialVersionUID = 1L;
		private Random rand = new Random();
		private int[] listA = {2, 3, 5};
		private int[] listB = {1, 4, 7};
		private int[] listC = {2, 2, 6};

		@Override
		public void invoke(Collector<ThreeNumbers> collector) throws Exception {
			for (int i = 0; i < listA.length; i++) {
				Thread.sleep(105L);
				collector.collect(new ThreeNumbers(listA[i], listB[i], listC[i]));
			}
		}
	}

	private static final class TwoSource implements SourceFunction<TwoNumbers> {
		private static final long serialVersionUID = 1L;
		private Random rand = new Random();
		private int[] listD = {3, 8, 5};
		private int[] listE = {4, 6, 7};

		@Override
		public void invoke(Collector<TwoNumbers> collector) throws Exception {
			for (int i = 0; i < listD.length; i++) {
				Thread.sleep(105L);
				collector.collect(new TwoNumbers(listD[i], listE[i]));
			}
		}
	}

	/*
	 * MAP CLASSES
	 */

	public static class ThreeMap implements MapFunction<ThreeNumbers, TwoNumbers> {
		private static final long serialVersionUID = 1L;

		public ThreeMap() {
		}

		@Override
		public TwoNumbers map(ThreeNumbers in) throws Exception {
			TwoNumbers result = new TwoNumbers();
			int d = (in.b * in.b) - (4 * in.a);
			int e = in.b + (3 * in.c * in.c);
			result.setD(d);
			result.setE(e);
			return result;
		}
	}

	public static class TwoMap implements MapFunction<TwoNumbers, Integer> {
		private static final long serialVersionUID = 1L;

		public TwoMap() {
		}

		@Override
		public Integer map(TwoNumbers in) throws Exception {
			return (3 * in.d) + in.e;
		}
	}

	public static class SumMap implements MapFunction<TwoNumbers, Integer> {
		private static final long serialVersionUID = 1L;

		public SumMap() {
		}

		@Override
		public Integer map(TwoNumbers in) throws Exception {
			return (3 * in.d) + in.e;
		}
	}

	public static class EmptyMap implements MapFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		public EmptyMap() {
		}

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}
	}

	/*
	 * FILTER CLASSES
	 */

	private static final class NegativeFilter implements FilterFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Integer value) throws Exception {
			return value > 0;
		}
	}

	/*
	 * SINK CLASSES
	 */

	public static class NumberSink implements SinkFunction<Integer> {
		private static final long serialVersionUID = 1L;

		public NumberSink() {
		}

		@Override
		public void invoke(Integer value) {
			System.err.println(String.valueOf(value));
		}
	}

	/*
	 * POJOs
	 */
	public static class ThreeNumbers {
		private int a;
		private int b;
		private int c;
		private long time;

		public ThreeNumbers() {
		}

		public ThreeNumbers(int a, int b, int c) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.time = System.nanoTime();
		}

		public int getA() {
			return a;
		}

		public void setA(int a) {
			this.a = a;
		}

		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}

		public int getC() {
			return c;
		}

		public void setC(int c) {
			this.c = c;
		}

	}

	public static class TwoNumbers {
		private int d;
		private int e;
		private long time;

		public TwoNumbers() {
		}

		public TwoNumbers(int d, int e) {
			this.d = d;
			this.e = e;
			this.time = System.nanoTime();
		}

		public int getD() {
			return d;
		}

		public void setD(int d) {
			this.d = d;
		}

		public int getE() {
			return e;
		}

		public void setE(int e) {
			this.e = e;
		}

	}

}
