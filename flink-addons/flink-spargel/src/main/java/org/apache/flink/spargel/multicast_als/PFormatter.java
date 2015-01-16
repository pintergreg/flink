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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class PFormatter
		implements
		GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, DoubleVectorWithMap>> {

	private DoubleVectorWithMap vectorWithMap = new DoubleVectorWithMap();

	@Override
	public void reduce(Iterable<Tuple3<Integer, Integer, Double>> elements,
			Collector<Tuple2<Integer, DoubleVectorWithMap>> out)
			throws Exception {
		Tuple3<Integer, Integer, Double> element = elements.iterator().next();

		// create the vertexId of this p column
		int id = IdFormatter.getVertexId(false, element.f0);
		Tuple2<Integer, DoubleVectorWithMap> vector = new Tuple2<Integer, DoubleVectorWithMap>(
				id, vectorWithMap);
		out.collect(vector);
	}

}