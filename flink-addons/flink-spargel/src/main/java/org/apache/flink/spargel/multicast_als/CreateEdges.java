/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CreateEdges
		implements
		FlatMapFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, Integer>> {

	@Override
	public void flatMap(Tuple3<Integer, Integer, Double> value,
			Collector<Tuple2<Integer, Integer>> out) throws Exception {
		int sourceId = IdFormatter.getVertexId(false, value.f0);
		int targetId = IdFormatter.getVertexId(true, value.f1);
		Tuple2<Integer, Integer> directed_edge = new Tuple2(sourceId, targetId);
		Tuple2<Integer, Integer> backward_edge = new Tuple2(targetId, sourceId);
		out.collect(directed_edge);
		out.collect(backward_edge);
	}

}
