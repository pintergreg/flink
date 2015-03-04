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

package org.apache.flink.streaming.api.ft.layer.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * SourceReplayer collects RecordWriters on the basis of SourceID, and emits serialized records
 */
public class SourceReplayer implements Collector<SemiDeserializedStreamRecord> {

	private static final Logger LOG = LoggerFactory.getLogger(SourceReplayer.class);

	private int sourceID;
	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> outputs;
	private SerializationDelegate<SemiDeserializedStreamRecord> serializationDelegate;
	private FTLayer ftLayer;

	public SourceReplayer(int sourceID,
			TypeSerializer<SemiDeserializedStreamRecord> typeSerializer, FTLayer ftLayer) {
		this.sourceID = sourceID;
		this.outputs = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>();
		this.serializationDelegate = new SerializationDelegate<SemiDeserializedStreamRecord>(
				typeSerializer);
		this.ftLayer = ftLayer;

		// TODO set it with serializer
	}

	/**
	 * Collects and emits a tuple/object to the outputs by reusing a
	 * StreamRecord object.
	 *
	 * @param outRecord
	 * 		Object to be collected and emitted.
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

	/**
	 * Adds a RecordWriter to the list of a RecordWriters of a source
	 * @param output
	 */
	public void addRecordWriter(RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output) {
		this.outputs.add(output);
	}

	public int getSourceID() {
		return this.sourceID;
	}
}
