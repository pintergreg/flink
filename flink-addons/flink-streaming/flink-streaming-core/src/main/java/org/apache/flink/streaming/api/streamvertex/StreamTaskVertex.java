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

package org.apache.flink.streaming.api.streamvertex;

import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.collector.StreamTaskCollector;
import org.apache.flink.streaming.api.ft.context.FTTaskContext;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.MutableObjectIterator;

public class StreamTaskVertex<IN, OUT> extends StreamVertex<IN, OUT> {

	protected InputHandler<IN> inputHandler;
	private StreamInvokable<IN, OUT> userInvokable;

	@Override
	protected void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);

		setFaultToleranceContext();

		collector = new StreamTaskCollector<OUT>(this, ftContext);
	}

	@Override
	protected void setInvokable() {
		userInvokable = getConfiguration().getUserInvokable(getUserClassLoader());
		userInvokable.setup(this, ftContext);
	}

	@Override
	protected StreamInvokable<IN, OUT> getInvokable() {
		return userInvokable;
	}

	@Override
	protected void createFTContext() {
		ftContext = new FTTaskContext(getPersistenceInput());
	}

	@Override
	public void initializeInvoke() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		if (index == 0) {
			return (MutableObjectIterator<X>) inputHandler.getInputIter();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		if (index == 0) {
			return (StreamRecordSerializer<X>) inputHandler.getInputSerializer();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		throw new IllegalArgumentException("CoReader not available");
	}

	public MutableRecordReader<DeserializationDelegate<StreamRecord<IN>>> getPersistenceInput() {
		return inputHandler.getPersistanceInput();
	}

}
