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

package org.apache.flink.streaming.api.invokable;

import org.apache.flink.streaming.api.function.sink.SinkFunction;

import java.util.HashSet;

public class SinkInvokable<IN> extends ChainableInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	private SinkFunction<IN> sinkFunction;

	//TODO ##ID_GEN amíg nem BF-et teszek ide teszelni
	private HashSet<Long> idStore;

	public SinkInvokable(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		this.sinkFunction = sinkFunction;

		this.idStore = new HashSet<Long>();
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		//TODO ide akarom a Bloom Filter ellenőrzést tenni
		//###ID_GEN
		if(!idStore.contains(nextRecord.getId().getCurrentRecordId())) {
			idStore.add(nextRecord.getId().getCurrentRecordId());

			sinkFunction.invoke(nextObject);
		}

	}

	@Override
	public void collect(IN record) {
		nextObject = copy(record);
		callUserFunctionAndLogException();
	}

}
