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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CreateVertices
		implements
		JoinFunction<Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>> {

	private DoubleVectorWithMap value = new DoubleVectorWithMap();
	private Tuple2<Integer, DoubleVectorWithMap> vertex = new Tuple2<Integer, DoubleVectorWithMap>();

	@Override
	public Tuple2<Integer, DoubleVectorWithMap> join(
			Tuple2<Integer, DoubleVectorWithMap> with_data,
			Tuple2<Integer, DoubleVectorWithMap> with_edges) throws Exception {
		value.setId(with_data.f0);
		value.setData(with_data.f1.getData());
		value.setEdges(with_edges.f1.getEdges());
		vertex.setField(with_data.f0, 0);
		vertex.setField(value, 1);
		return vertex;
	}

}