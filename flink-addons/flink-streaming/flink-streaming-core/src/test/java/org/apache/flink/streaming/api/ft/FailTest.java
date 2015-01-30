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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.ft.layer.event.FailException;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Test;

import java.io.Serializable;

public class FailTest {

	public class MyMapFunction implements MapFunction<Long, Long>, Serializable {
		private Long z = 3L;

		@Override
		public Long map(Long value) throws Exception {
			if (value.equals(z)) {
				z = -1L;
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
		StreamExecutionEnvironment env = new TestStreamEnvironment(1, 32);
		env.setDegreeOfParallelism(1);

		env.generateSequence(0, 5).map(new MyMapFunction())
				.addSink(new MySink());

		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
