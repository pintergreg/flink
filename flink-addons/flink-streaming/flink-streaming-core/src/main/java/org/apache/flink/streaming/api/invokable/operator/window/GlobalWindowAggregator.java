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

package org.apache.flink.streaming.api.invokable.operator.window;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class GlobalWindowAggregator<IN> extends StreamInvokable<Tuple3<IN, Integer, Boolean>, IN> {

	private static final long serialVersionUID = 1L;

	private ReduceFunction<IN> reducer;
	private int preAggregateParallelism;

	private Map<Integer, Tuple2<IN, Integer>> aggregates;

	public GlobalWindowAggregator(ReduceFunction<IN> reducer, int preAggregateParallelism) {
		super(reducer);
		this.reducer = reducer;
		this.preAggregateParallelism = preAggregateParallelism;
		this.aggregates = new HashMap<Integer, Tuple2<IN, Integer>>();
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {

			Integer windowID = nextObject.f1;
			IN preAggregate = nextObject.f0;
			Boolean isEmptyWindow = nextObject.f2;

			Tuple2<IN, Integer> aggregate = aggregates.get(windowID);

			if (aggregate != null) {
				if (isEmptyWindow) {
					if (aggregate.f0 != null) {
						aggregate.f0 = reducer.reduce(aggregate.f0, preAggregate);
					} else {
						aggregate.f0 = preAggregate;
					}
				}
				aggregate.f1--;
				if (aggregate.f1 == 0) {
					collector.collect(aggregate.f0);
					aggregates.remove(windowID);
				}
			} else {
				aggregate = new Tuple2<IN, Integer>();
				if (isEmptyWindow) {
					aggregate.f0 = preAggregate;
				} else {
					aggregate.f0 = null;
				}
				aggregate.f1 = preAggregateParallelism - 1;
				if (aggregate.f1 == 0) {
					collector.collect(aggregate.f0);
				} else {
					aggregates.put(windowID, aggregate);
				}
			}

		}
	}
}
