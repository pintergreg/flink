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

package org.apache.flink.streaming.api.collector.ft;

import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.AbstractStreamCollector;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For collecting failed records to tasks successives to a given source.
 */
public class FailedRecordCollector implements Collector<SemiDeserializedStreamRecord> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCollector.class);

	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> outputs;
	private SerializationDelegate<SemiDeserializedStreamRecord> serializationDelegate;
	private FTLayer ftLayer;

	/**
	 * Creates a new StreamCollector
	 * 
	 * @param channelID
	 *            Channel ID of the Task
	 * @param serializationDelegate
	 *            Serialization delegate used for serialization
	 */
	public FailedRecordCollector(
			List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> outputs,
			TypeSerializer<SemiDeserializedStreamRecord> typeSerializer, FTLayer ftLayer) {
		//TypeSerializer<SemiDeserializedStreamRecord> typeSerializer = new AsStreamRecordSerializer();
		this.outputs = outputs;
		this.serializationDelegate = new SerializationDelegate<SemiDeserializedStreamRecord>(
				typeSerializer);
		this.ftLayer = ftLayer;
		// TODO set it with serializer
		// this.serializationDelegate = ...
	}

	/**
	 * Collects and emits a tuple/object to the outputs by reusing a
	 * StreamRecord object.
	 * 
	 * @param outRecord
	 *            Object to be collected and emitted.
	 */
	@Override
	public void collect(SemiDeserializedStreamRecord outRecord) {
		serializationDelegate.setInstance(outRecord);

		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output : outputs) {
			try {
				RecordId newRecordId = setOutRecordId(serializationDelegate);
				output.emit(serializationDelegate);
				ftLayer.xor(newRecordId);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit failed due to: {}", StringUtils.stringifyException(e));
				}
			}
		}
	}

	protected RecordId setOutRecordId(SerializationDelegate<SemiDeserializedStreamRecord> outRecord) {
		long sourceRecordId = outRecord.getInstance().getId().getSourceRecordId();
		RecordId newSourceRecordId = RecordId.newRecordId(sourceRecordId);
		outRecord.getInstance().setId(newSourceRecordId);
		return newSourceRecordId;
	}

	@Override
	public void close() {
	}
}