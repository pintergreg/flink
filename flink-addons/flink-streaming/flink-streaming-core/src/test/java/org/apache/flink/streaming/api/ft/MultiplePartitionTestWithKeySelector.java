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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class MultiplePartitionTestWithKeySelector {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Word> source1 = env.addSource(new TextSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		DataStream<String> source2 = env.addSource(new RandomWordSource()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

//		// this would be a good old word count
//		source1.groupBy(new WordKeySelector()).sum("count").print();

		// this one is a more complex topology, with much less sense
		// contains 2 sources, with 4 outputs (3 different type)
		source1.filter(new WordFilter()).merge(source2.shuffle().filter(new StringFilter()).map(new StringToWordMap())).
				groupBy(new WordKeySelector()).sum("count").addSink(new WordSink());

		source1.groupBy(new WordKeySelector()).map(new WordToStringMap()).merge(source2.filter(new StringFilter())).addSink(new StringSink());

		source1.distribute();

		// execute program
		env.execute("Streaming WordCount");
	}

	/*
	 * SOURCE CLASSES
	 */

	private static final class TextSource implements SourceFunction<Word> {
		private static final long serialVersionUID = 1L;

		private static final String[] text = {"an elephant shaped storm cloud appeared above",
				"the valley where the yellow elephant lives",
				"the house of the yellow elephant is made of bamboo",
				"that grows in the valley",
				"this yellow elephant does not like storm",
				"because he is afraid of bolts",
				"so when he saw the the elephant shaped storm cloud",
				"he quickly went into his house hiding from the storm"
		};
		/*
		the pink squirrel headed home because of the oncoming storm with a nice piece of acorn, but he
		thought he would visit his old friend. The pink squirrel found him hiding in his bamboo house
		 */

		@Override
		public void invoke(Collector<Word> collector) throws Exception {
			for (String line : text) {
				for (String word : line.split(" ")) {
					collector.collect(new Word(word, 1));
				}
			}
		}
	}

	private static final class RandomWordSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private static final String[] words = {"elephant", "storm", "cloud", "yellow",
				"house", "bamboo", "valley"};

		@Override
		public void invoke(Collector<String> collector) throws Exception {
			Random rand = new Random();
			for (int i = 0; i < 10; i++) {
				collector.collect(words[rand.nextInt(words.length)]);
			}
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

	/*
	 * FILTER CLASSES
	 */

	private static final class WordFilter implements FilterFunction<Word> {
		private static final long serialVersionUID = 1L;

		private final String[] blackListWords = {"he", "his"};
		ArrayList<String> blackList = new ArrayList<String>(Arrays.asList(blackListWords));

		@Override
		public boolean filter(Word value) throws Exception {
			return !blackList.contains(value.getWord());
		}
	}

	private static final class StringFilter implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		private final String[] blackListWords = {"house", "home"};
		ArrayList<String> blackList = new ArrayList<String>(Arrays.asList(blackListWords));

		@Override
		public boolean filter(String value) throws Exception {
			return !blackList.contains(value);
		}
	}

	/*
	 * MAP CLASSES
	 */

	public static class StringToWordMap implements MapFunction<String, Word> {
		private static final long serialVersionUID = 1L;

		public StringToWordMap() {
		}

		@Override
		public Word map(String value) throws Exception {
			return new Word(value, 1);
		}
	}

	public static class WordToStringMap implements MapFunction<Word, String> {
		private static final long serialVersionUID = 1L;

		public WordToStringMap() {
		}

		@Override
		public String map(Word value) throws Exception {
			return value.getWord();
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
			System.err.println("[" + value.getWord() + ", " + value.getCount() + "]");
		}
	}

	public static class StringSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		public StringSink() {

		}

		@Override
		public void invoke(String value) {
			System.out.println(value);
		}
	}

}
