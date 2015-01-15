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

import org.apache.flink.streaming.api.collector.StreamTaskCollector;
import org.apache.flink.streaming.api.collector.ft.TaskAckerCollector;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.MutableObjectIterator;

public class CoStreamVertex<IN1, IN2, OUT> extends StreamVertex<IN1, OUT> {

	private CoInvokable<IN1, IN2, OUT> userInvokable;
	private CoInputHandler<IN1, IN2> coInputHandler;

	public CoStreamVertex() {
		super();
		userInvokable = null;
	}

	@Override
	protected void setInputsOutputs() {
		coInputHandler = new CoInputHandler<IN1, IN2>(this);
		ackerCollector = new TaskAckerCollector(coInputHandler.getPersistanceInput());
		collector = new StreamTaskCollector<OUT>(this, ackerCollector);
	}

	@Override
	protected void setInvokable() {
		userInvokable = getConfiguration().getUserInvokable(getUserClassLoader());

		userInvokable.setup(this);
	}

	@Override
	protected CoInvokable<IN1, IN2, OUT> getInvokable() {
		return userInvokable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		switch (index) {
		case 0:
			return (StreamRecordSerializer<X>) coInputHandler.getInputDeserializer1();
		case 1:
			return (StreamRecordSerializer<X>) coInputHandler.getInputDeserializer2();
		default:
			throw new IllegalArgumentException("CoStreamVertex has only 2 inputs");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		return (CoReaderIterator<X, Y>) coInputHandler.getCoInputIter();
	}

	@Override
	public void initializeInvoke() {
	}

}
