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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;


public class PerformanceCompareTest {

	private static final Logger LOG = LoggerFactory.getLogger(PerformanceCompareTest.class);

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


		DataStream<ThreeNumbers> sourceStream1 = env.addSource(new ThreeSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		sourceStream1.map(new ThreeMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.map(new TwoMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.map(new FinalMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				//.addSink(new KafkaSink())
				.addSink(new StringSink())
		;

		//run this topology
		env.execute();
	}


	/*
	 * SOURCE CLASSES
	 */

	private static final class ThreeSource implements SourceFunction<ThreeNumbers> {
		private static final long serialVersionUID = 1L;
		private Random rand = new Random();
		long start=System.nanoTime();

		@Override
		public void invoke(Collector<ThreeNumbers> collector) throws Exception {
			for (int i = 0; i < 50000; i++) {
				collector.collect(new ThreeNumbers(rand.nextInt(9) + 1, rand.nextInt(9) + 1, rand.nextInt(9) + 1));
			}
//			while(System.nanoTime()<start+12000000000L){
//				collector.collect(new ThreeNumbers(rand.nextInt(9) + 1, rand.nextInt(9) + 1, rand.nextInt(9) + 1));
//			}
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
			int d = (in.b * in.b) - (4 * in.a);
			int e = in.b + (3 * in.c * in.c);

			return new TwoNumbers(d, e, in.getTime());
		}
	}

	public static class TwoMap implements MapFunction<TwoNumbers, OneNumber> {
		private static final long serialVersionUID = 1L;

		public TwoMap() {
		}

		@Override
		public OneNumber map(TwoNumbers in) throws Exception {
			return new OneNumber((3 * in.d) + in.e, in.getTime());
		}
	}

	public static class FinalMap implements MapFunction<OneNumber, String> {
		private static final long serialVersionUID = 1L;

		public FinalMap() {
		}

		@Override
		public String map(OneNumber in) throws Exception {
			return in.toString();
		}
	}

	/*
	 * SINK CLASSES
	 */

	public static class NumberSink implements SinkFunction<OneNumber> {
		private static final long serialVersionUID = 1L;

		public NumberSink() {
		}

		@Override
		public void invoke(OneNumber value) {
			System.err.println(value.toString());
		}
	}

	public static class StringSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;
		private ArrayList<String> list = new ArrayList<String>();

		public StringSink() {
		}

		@Override
		public void invoke(String value) {
			LOG.warn("[PERF] " + value);

		}
	}

	/*
	 * POJOs
	 */
	public static class ThreeNumbers implements Serializable{
		private static final long serialVersionUID = 1L;
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

		public long getTime() {
			return this.time;
		}

		public void setTime(long time) {
			this.time = time;
		}
	}

	public static class TwoNumbers implements Serializable{
		private static final long serialVersionUID = 1L;
		private int d;
		private int e;
		private long time;

		public TwoNumbers() {
		}

		public TwoNumbers(int d, int e, long time) {
			this.d = d;
			this.e = e;
			this.time = time;
		}

		public TwoNumbers(int d, int e) {
			new TwoNumbers(d, e, System.nanoTime());
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

		public long getTime() {
			return this.time;
		}

		public void setTime(long time) {
			this.time = time;
		}
	}

	public static class OneNumber implements Serializable{
		private static final long serialVersionUID = 1L;
		private int f;
		private long time;

		public OneNumber() {
		}

		public OneNumber(int f, long time) {
			this.f = f;
			this.time = System.nanoTime();
		}

		public int getF() {
			return f;
		}

		public void setF(int f) {
			this.f = f;
		}

		public long getTime() {
			return this.time;
		}

		public void setTime(long time) {
			this.time = time;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.f);
			sb.append(", time:");
			sb.append((System.nanoTime() - this.time) / 1000000);
			sb.append("ms");
			return sb.toString();
		}

	}

}
