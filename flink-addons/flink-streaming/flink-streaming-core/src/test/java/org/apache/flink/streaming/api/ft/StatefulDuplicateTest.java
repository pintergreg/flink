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

import org.apache.flink.api.common.functions.RichStateMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.flink.streaming.state.SimpleState;
import org.apache.flink.streaming.util.ExactlyOnceParameters;
import org.apache.flink.util.Collector;

public class StatefulDuplicateTest {

	public static void main(String[] args) throws Exception {

		numberSequenceWithoutShuffle();
	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */

	private static void numberSequenceWithoutShuffle() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		env.setExactlyOnceExecution(new ExactlyOnceParameters(1000000, 0.000001, 5000));
		//env.disableExactlyOnceExecution();

		// building the job graph
		/*
		*  (So)--(M)--(Si)
		*
		* Source emits numbers from 1 to 10
		* Map converts number to text
		* Sink prints values to standard error output
		* 10!=3628800
		*/
		SimpleState<Long> ize = new SimpleState<Long>(1L);

		DataStream<Integer> sourceStream1 = env.addSource(new NumberSource(1, 10)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		sourceStream1.map(new NumberMap()).registerState("factorial", ize)
		.setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).addSink(new SimpleSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		//run this topology
		env.execute();
	}


	/*
	 * SOURCE CLASSES
	 */

	private static final class NumberSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private int from;
		private int to;

		public NumberSource(int from, int to) {
			this.from = from;
			this.to = to;
		}

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {
			for (int i = from; i <= to; i++) {
				Thread.sleep(1005L);
				collector.collect(i);
			}
		}
	}

	/*
	 * MAP CLASSES
	 */

	public static class NumberMap extends RichStateMapFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		private static OperatorState<Long> state;

		public NumberMap() {
		}

		@Override
		public String map(Integer value, boolean replayed) throws Exception {
			if (!replayed) {
				state.setState(state.getState() * value);
			}

			System.out.println("STATE:" + state.getState() + " after the value of:" + value);
			return value.toString();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			state = (OperatorState<Long>) ((StreamingRuntimeContext) getRuntimeContext()).getState("factorial");
		}

	}

	/*
	 * SINK CLASSES
	 */

	public static class SimpleSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		public SimpleSink() {
		}

		@Override
		public void invoke(String value) {
			System.err.println(value);
		}
	}

}
