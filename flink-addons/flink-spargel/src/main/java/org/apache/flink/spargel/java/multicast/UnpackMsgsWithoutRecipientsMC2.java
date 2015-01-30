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
package org.apache.flink.spargel.java.multicast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

public class UnpackMsgsWithoutRecipientsMC2<VertexKey extends Comparable<VertexKey>, Message>
		extends
		RichCoGroupFunction<Tuple4<VertexKey, VertexKey, Integer, VertexKey>, Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>> {

	private static final long serialVersionUID = 1L;
	private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;

	public UnpackMsgsWithoutRecipientsMC2(
			TypeInformation<Tuple2<VertexKey, Message>> resultType) {
		this.resultType = resultType;
	}

	@Override
	public TypeInformation<Tuple2<VertexKey, Message>> getProducedType() {
		return this.resultType;
	}

	private Tuple2<VertexKey, Message> reuse = new Tuple2<VertexKey, Message>();

	private Map<VertexKey, List<VertexKey>> outNeighboursInThisPart = new HashMap<VertexKey, List<VertexKey>>();

	@Override
	public void coGroup(
			Iterable<Tuple4<VertexKey, VertexKey, Integer, VertexKey>> edgesInPart,
			Iterable<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> messages,
			Collector<Tuple2<VertexKey, Message>> out) throws Exception {
		if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
			// read outneighbours into memory
			for (Tuple4<VertexKey, VertexKey, Integer, VertexKey> edge : edgesInPart) {
				VertexKey source = edge.f0;
				VertexKey target = edge.f1;
				if (!outNeighboursInThisPart.containsKey(source)) {
					outNeighboursInThisPart.put(source,
							new ArrayList<VertexKey>());
				}
				outNeighboursInThisPart.get(source).add(target);
			}
			// System.out.println("Subtask: " +
			// getIterationRuntimeContext().getIndexOfThisSubtask());
			// System.out.println(outNeighboursInThisPart);
		}
		for (Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>> m : messages) {
			reuse.f1 = m.f1.getMessage();
			VertexKey sender = m.f1.getSender();
			for (VertexKey recipient : outNeighboursInThisPart.get(sender)) {
				reuse.f0 = recipient;
				out.collect(reuse);
			}
		}
	}
}
