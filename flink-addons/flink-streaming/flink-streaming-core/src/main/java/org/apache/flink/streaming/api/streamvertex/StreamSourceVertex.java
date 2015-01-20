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

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.StreamSourceCollector;
import org.apache.flink.streaming.api.collector.ft.PersistenceCollector;
import org.apache.flink.streaming.api.collector.ft.SourceAckerCollector;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.PersistencePartitioner;
import org.apache.flink.util.MutableObjectIterator;

public class StreamSourceVertex<OUT> extends StreamVertex<OUT, OUT> {

	private SourceInvokable<OUT> sourceInvokable;
	private PersistenceCollector persistenceCollector;
	private StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> ftWriter;
	
	@Override
	protected void setInputsOutputs() {
		PersistencePartitioner partitioner = new PersistencePartitioner();
		ftWriter = new StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(this, partitioner, 100L);

		persistenceCollector = new PersistenceCollector(ftWriter);
		ackerCollector = new SourceAckerCollector(ftWriter);

		collector = new StreamSourceCollector<OUT>(this, ackerCollector, persistenceCollector);
	}

	@Override
	protected void setInvokable() {
		sourceInvokable = getConfiguration().getUserInvokable(getUserClassLoader());
		sourceInvokable.setup(this);
	}

	@Override
	protected StreamInvokable<OUT, OUT> getInvokable() {
		return sourceInvokable;
	}

	@Override
	public void initializeInvoke() {
		ftWriter.initializeSerializers();
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