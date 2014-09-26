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

package org.apache.flink.streaming.api;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LambdaEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class LambdaTest implements Serializable {

	private static final long serialVersionUID = 1L;

	public static class Increment implements MapFunction<Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) throws Exception {
			return value + 1;
		}

	}

	@Test
	public void test() throws Exception {
		ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment(1);
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
		
		DataSet<Long> dataSet = batchEnv.generateSequence(1, 10).map(new Increment());

		DataStream<Long> dataStream = streamEnv.generateSequence(10, 20);
		
		dataStream.lambdaJoin(dataSet).map(new Increment()).print();
		dataSet.print();
		
		LambdaEnvironment lambdaEnv = new LambdaEnvironment(batchEnv, streamEnv);
		lambdaEnv.execute(":)");

	}
}
