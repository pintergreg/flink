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

package org.apache.flink.streaming.api.collector;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.context.FTContext;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;

/**
 * Collector for tuples in Apache Flink stream processing. The collected values
 * will be wrapped with ID in a {@link StreamRecord} and then emitted to the
 * outputs.
 *
 * @param <T> Type of the Tuples/Objects collected.
 */
public class StreamTaskCollector<T> extends
		AbstractStreamCollector<T, SerializationDelegate<StreamRecord<T>>> {

	protected StreamRecord<T> streamRecord;
	protected SerializationDelegate<StreamRecord<T>> serializationDelegate;

	/**
	 * Creates a new StreamCollector
	 *
	 * @param channelID             Channel ID of the Task
	 * @param serializationDelegate
	 * @param streamComponent
	 * @param ackerCollector
	 */
	public StreamTaskCollector(StreamVertex<?, T> streamComponent, FTContext ftContext) {
		super(streamComponent, ftContext);

		serializationDelegate = new SerializationDelegate<StreamRecord<T>>(outSerializer);
		serializationDelegate.setInstance(outSerializer != null ? outSerializer.createInstance() : null);

		if (serializationDelegate != null) {
			this.streamRecord = serializationDelegate.getInstance();
		} else {
			this.streamRecord = new StreamRecord<T>();
		}
	}

	public StreamTaskCollector(StreamTaskCollector<T> other) {
		super(other);
		this.streamRecord = other.streamRecord;
		this.serializationDelegate = other.serializationDelegate;
	}

	public void setAnchorRecord(RecordId anchorRecord) {
		this.anchorRecord = anchorRecord;
	}

	@Override
	protected SerializationDelegate<StreamRecord<T>> produceOutRecord(T outObject) {

		streamRecord.setObject(outObject);
		serializationDelegate.setInstance(streamRecord);

		return serializationDelegate;
	}

	@Override
	protected RecordId setOutRecordId(SerializationDelegate<StreamRecord<T>> outRecord,
			RecordId recordId) {

		RecordId xorMessageNewRecord = RecordId.newRecordId(recordId.getSourceRecordId());

		outRecord.getInstance().setId(xorMessageNewRecord);
		return xorMessageNewRecord;
	}
}
