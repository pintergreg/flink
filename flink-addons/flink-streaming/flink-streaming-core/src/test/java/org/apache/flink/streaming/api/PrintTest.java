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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Test;

public class PrintTest implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final long MEMORYSIZE = 32;

	private static final class IdentityMap implements MapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) throws Exception {
			return value;
		}
	}

	private static final class FilterAll implements FilterFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Long value) throws Exception {
			return true;
		}
	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(1, MEMORYSIZE);
		//env.generateSequence(1, 10).map(new IdentityMap()).filter(new FilterAll()).print();

		env.fromElements(new Count("flink", 2015)).project("f0").typesNew(String.class).print();

		env.execute();
	}

	public static class Count extends Tuple2<String, Integer> {

		public Count() {
		}

		public Count(String f0, Integer f1) {
			this.f0 = f0;
			this.f1 = f1;
		}

		public String getF0() {
			return f0;
		}

		public void setF0(String f0) {
			this.f0 = f0;
		}

		public Integer getF1() {
			return f1;
		}

		public void setF1(Integer f1) {
			this.f1 = f1;
		}

	}
}
