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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

public class UnpackMsgsWithRecipientsMC1<VertexKey extends Comparable<VertexKey>, Message>
		extends
		RichFlatMapFunction<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>> {

	private static final long serialVersionUID = 1L;
	private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;

	public UnpackMsgsWithRecipientsMC1(
			TypeInformation<Tuple2<VertexKey, Message>> resultType) {
		this.resultType = resultType;
	}

	Tuple2<VertexKey, Message> reuse = new Tuple2<VertexKey, Message>();

	@Override
	public void flatMap(
			Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>> value,
			Collector<Tuple2<VertexKey, Message>> out) throws Exception {
		reuse.f1 = value.f1.getMessage();
		for (VertexKey target : value.f1.getSomeRecipients()) {
			reuse.f0 = target;
			out.collect(reuse);
		}

	}

	@Override
	public TypeInformation<Tuple2<VertexKey, Message>> getProducedType() {
		return this.resultType;
	}
}
