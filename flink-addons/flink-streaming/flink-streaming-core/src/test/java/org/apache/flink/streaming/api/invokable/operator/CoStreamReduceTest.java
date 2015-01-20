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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.junit.Test;

public class CoStreamReduceTest {

	public static class MyCoReduceFunction implements CoReduceFunction<Long, String, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce1(Long value1, Long value2) {
			return value1 * value2;
		}

		@Override
		public String reduce2(String value1, String value2) {
			return value1 + value2;
		}

		@Override
		public Long map1(Long value) {
			return value;
		}

		@Override
		public Long map2(String value) {
			return Long.parseLong(value);
		}

	}

	@Test
	public void coStreamReduceTest() {

//		CoReduceInvokable<Integer, String, Integer> coReduce = new CoReduceInvokable<Integer, String, Integer>(
//				new MyCoReduceFunction());
//
//		List<Integer> expected1 = Arrays.asList(1, 9, 2, 99, 6, 998, 24);
//		List<Integer> result = MockCoInvokable.createAndExecute(coReduce,
//				Arrays.asList(1, 2, 3, 4), Arrays.asList("9", "9", "8"));
//
//		assertEquals(expected1, result);

//		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
//		env.generateSequence(1, 10)
//				.connect(env.fromElements("1", "2", "3", "4", "5")).batch(4, 4, 2, 2)
//				.reduce(new MyCoReduceFunction()).print();
//		try {
//			env.execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		List<Integer> expected1 = Arrays.asList(1, 9, 2, 99, 6, 998, 24);
//		List<Integer> result = MockCoContext.createAndExecute(coReduce,
//				Arrays.asList(1, 2, 3, 4), Arrays.asList("9", "9", "8"));
//
//		assertEquals(expected1, result);

	}
}
