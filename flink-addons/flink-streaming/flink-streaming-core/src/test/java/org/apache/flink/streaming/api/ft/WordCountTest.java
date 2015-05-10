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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.util.ExactlyOnceParameters;
import org.apache.flink.util.Collector;

/**
 * Test created for testing edge information gathering and replaypartition setting
 */
public class WordCountTest {

	public static void main(String[] args) throws Exception {

		wourdCount();
	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */

	private static void wourdCount() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);
		env.setExactlyOnceExecution(new ExactlyOnceParameters(1000000, 0.000001, 5000));
		//env.disableExactlyOnceExecution();
		env.setReplayTimeout(10L);

		DataStream<Word> source1 = env.addSource(new TextSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

		source1.groupBy(new WordKeySelector()).sum("count").setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.addSink(new WordSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		///source1.groupBy(new WordKeySelector()).reduce(new MyReduce()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
		///		.addSink(new WordSink());

		//run this topology
		env.execute();
	}

	/*
	 * SOURCE CLASSES
	 */

	private static final class TextSource implements SourceFunction<Word> {
		private static final long serialVersionUID = 1L;

//		private static final String[] text = {"an elephant shaped storm cloud appeared above",
//				"the valley where the yellow elephant lives",
//				"the house of the yellow elephant is made of bamboo",
//				"that grows in the valley",
//				"this yellow elephant does not like storm",
//				"because he is afraid of bolts",
//				"so when he saw the the elephant shaped storm cloud",
//				"he quickly went into his house hiding from the storm"
//		};
		/*
		the pink squirrel headed home because of the oncoming storm with a nice piece of acorn, but he
		thought he would visit his old friend. The pink squirrel found him hiding in his bamboo house.
		The yellow elephant was glad, because the coming of his friend.
		 */

		private static final String[] text = {"mókus mókus mókus mókus"};

		@Override
		public void invoke(Collector<Word> collector) throws Exception {
			for (String line : text) {
				for (String word : line.split(" ")) {
					Thread.sleep(1000L);
					collector.collect(new Word(word, 1));
					Thread.sleep(1000L);
				}
			}
		}
	}

//	public static final class Tokenizer implements StatefulFlatMapFunction<String, Tuple2<String, Integer>> {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void flatMap(String inTuple, Collector<Tuple2<String, Integer>> out, boolean replayed)
//				throws Exception {
//			// tokenize the line
//			StringTokenizer tokenizer = new StringTokenizer(inTuple);
//
//			// emit the pairs
//			while (tokenizer.hasMoreTokens()) {
//				if (replayed) {
//					out.collect(new Tuple2<String, Integer>(tokenizer.nextToken(), 1));
//				}else{
//					out.collect(new Tuple2<String, Integer>(tokenizer.nextToken(), 1));
//				}
//			}
//		}
//	}

	public static class MyReduce implements ReduceFunction<Word>{

		@Override
		public Word reduce(Word w1, Word w2) throws Exception {
//			if (w1.word.equals(w2.word))
//			return new Word(w1.word, w1.count+w2.count);
//			else
				return new Word("GORRAM",0);
		}
	}

	/*
	 * SINK CLASSES
	 */

	public static class WordSink implements SinkFunction<Word> {
		private static final long serialVersionUID = 1L;

		public WordSink() {
		}

		@Override
		public void invoke(Word value) {
			System.err.println(value.toString());
		}
	}

	/*
	 * WORD POJO AND ITS KEYSELECTOR
	 */

	public static class Word {

		private String word;
		private int count;

		public Word() {
		}

		public Word(String word, int count) {
			this.word = word;
			this.count = count;
		}

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		@Override
		public String toString() {
			return "<" + this.word + ", " + String.valueOf(this.count) + ">";
		}

	}

	private static final class WordKeySelector implements KeySelector<Word, String> {

		@Override
		public String getKey(Word w) throws Exception {
			return w.word;
		}
	}

}
