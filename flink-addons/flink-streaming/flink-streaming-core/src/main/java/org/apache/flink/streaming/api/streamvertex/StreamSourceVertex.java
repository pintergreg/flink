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

import org.apache.flink.streaming.api.collector.StreamSourceCollector;
import org.apache.flink.streaming.api.ft.context.FTSourceContext;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.MutableObjectIterator;

public class StreamSourceVertex<OUT> extends StreamVertex<OUT, OUT> {

	private SourceInvokable<OUT> sourceInvokable;

	private FTSourceContext ftSourceContext;

	@Override
	protected void setInputsOutputs() {
		setFaultToleranceContext();

		collector = new StreamSourceCollector<OUT>(this, ftSourceContext); //ackerCollector, persistenceCollector);
	}

	@Override
	protected void setInvokable() {
		sourceInvokable = getConfiguration().getUserInvokable(getUserClassLoader());
		
		sourceInvokable.setup(this, ftContext);
	}

	@Override
	protected StreamInvokable<OUT, OUT> getInvokable() {
		return sourceInvokable;
	}

	@Override
	protected void createFTContext() {
		ftSourceContext = new FTSourceContext(this);
		ftContext = ftSourceContext;
	}

	@Override
	public void initializeInvoke() {
		ftContext.initialize();
	}

	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		return null;
	}

	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		return null;
	}

	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		throw new IllegalArgumentException("CoReader not available");
	}
}