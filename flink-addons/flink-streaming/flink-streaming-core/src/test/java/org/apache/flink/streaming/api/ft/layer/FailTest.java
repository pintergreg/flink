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

package org.apache.flink.streaming.api.ft.layer;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.invokable.ft.FailException;
import org.junit.Test;

public class FailTest {

	public class MyMapFunction implements MapFunction<Integer, Integer>, Serializable {
		private Integer z = 3;

		@Override
		public Integer map(Integer value) throws Exception {
			if (value == z) {
				z = -1;
				throw new FailException();
			} else {
				return value;
			}
		}
	}
	
	public static class MySink implements SinkFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Long value) {

		}
	}

	@Test
	public void test() {
//		MockLocalStreamEnvironment env = new MockLocalStreamEnvironment();
//		env.setDegreeOfParallelism(1);
//
//		env.generateSequence(0, 5).setParallelism(1).map(new MyMapFunction())
//				.addSink(new MySink());
//
//		try {
//			env.executeTest(32);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
