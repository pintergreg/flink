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

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.sink.SinkFunction;

public class SinkInvokable<IN> extends ChainableInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	private SinkFunction<IN> sinkFunction;
	int counter=0;
	long startTime;
	long stopTime;

	public SinkInvokable(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		this.sinkFunction = sinkFunction;
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		sinkFunction.invoke(nextObject);
		counter++;
	}

	@Override
	public void collect(IN record) {
		nextObject = copy(record);
		callUserFunctionAndLogException();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		isRunning = true;
		FunctionUtils.openFunction(userFunction, parameters);
		startTime =System.nanoTime();
		stopTime=startTime+10000000000L;
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
			System.err.println(String.valueOf(counter)+" in:"+ String.valueOf(System.nanoTime()- startTime));
		}
	}

}
