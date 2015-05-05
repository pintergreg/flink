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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichStateMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.util.ExactlyOnceParameters;
import pintergreg.bloomfilter.A2BloomFilter;

public class MapInvokable<IN, OUT> extends ChainableInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	private MapFunction<IN, OUT> mapper;

	private A2BloomFilter bloomFilter;
	private boolean isExactlyOnce = false;

	public MapInvokable(MapFunction<IN, OUT> mapper) {
		super(mapper);
		this.mapper = mapper;
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {

		//###DOCUMENT
		if (mapper instanceof RichStateMapFunction) {
			if (this.isExactlyOnce) {
				if (!bloomFilter.include(nextRecord.getId().getCurrentRecordId())) {
					bloomFilter.add(nextRecord.getId().getCurrentRecordId());
					collector.collect((OUT) ((RichStateMapFunction) mapper).map(nextObject, false));
				} else {
					collector.collect((OUT) ((RichStateMapFunction) mapper).map(nextObject, true));
				}
			}else {
				collector.collect(mapper.map(nextObject));
			}
		} else {
			collector.collect(mapper.map(nextObject));
		}

	}

	@Override
	public void collect(IN record) {
		nextObject = copy(record);
		callUserFunctionAndLogException();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		isRunning = true;
		this.isExactlyOnce = taskContext.getConfig().getExactlyOnce();
		if (this.isExactlyOnce) {
			ExactlyOnceParameters p = taskContext.getConfig().getExactlyOnceParameters();
			this.bloomFilter = new A2BloomFilter(p.getN(), p.getP(), p.getTtl());
		}
		FunctionUtils.openFunction(userFunction, parameters);
	}

	@Override
	public void close() {
		isRunning = false;
		collector.close();
		try {
			FunctionUtils.closeFunction(userFunction);
		} catch (Exception e) {
			throw new RuntimeException("Error when closing the function: " + e.getMessage());
		} finally {
			if (this.isExactlyOnce) {
				this.bloomFilter.stopTimer();
			}
		}
	}
}
