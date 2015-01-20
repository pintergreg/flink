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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.context.FTSourceContext;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;

public class StreamSourceCollector<T> extends
		AbstractStreamCollector<T, SerializationDelegate<SemiDeserializedStreamRecord>> {

	protected DataOutputSerializer temporaryOutputView;
	protected SemiDeserializedStreamRecord semiDeserializedStreamRecord;

	protected SerializationDelegate<SemiDeserializedStreamRecord> serializationDelegate;

	protected TypeSerializer<T> objectSerializer;

	private FTSourceContext ftSourceContext;

	public StreamSourceCollector(StreamVertex<?, T> streamComponent, FTSourceContext ftContext) {
		super(streamComponent, ftContext);

		this.ftSourceContext = ftContext;

		this.serializationDelegate = SemiDeserializedStreamRecord.createSerializationDelegate();

		if (serializationDelegate.getInstance() != null) {
			this.semiDeserializedStreamRecord = serializationDelegate.getInstance();
		} else {
			this.semiDeserializedStreamRecord = new SemiDeserializedStreamRecord();
		}

		TypeInformation<T> outType = TypeExtractor.getForObject(outSerializer.createInstance().getObject());
		objectSerializer = outType.createSerializer();

		temporaryOutputView = new DataOutputSerializer(outSerializer.getLength() < 0 ? 64
				: outSerializer.getLength());
	}

	public StreamSourceCollector(StreamSourceCollector<T> other) {
		super(other);
		this.ftSourceContext = other.ftSourceContext;
		this.outSerializer = other.outSerializer;

		this.serializationDelegate = other.serializationDelegate;

		this.semiDeserializedStreamRecord = other.semiDeserializedStreamRecord;
	}

	@Override
	protected SerializationDelegate<SemiDeserializedStreamRecord> produceOutRecord(T outObject) {
		semiDeserializedStreamRecord.setSerializedRecord(getSerializedObject(outObject));
		semiDeserializedStreamRecord.setId(RecordId.newSourceRecordId());
		semiDeserializedStreamRecord.setHashCode(outObject.hashCode());

		serializationDelegate.setInstance(semiDeserializedStreamRecord);

		ftSourceContext.persist(semiDeserializedStreamRecord);

		return serializationDelegate;
	}

	private ByteBuffer getSerializedObject(T object) {
		try {
			temporaryOutputView.clear();
			objectSerializer.serialize(object, temporaryOutputView);
		} catch (IOException e) {
			throw new RuntimeException("Cannot serialize object", e);
		}

		ByteBuffer result = temporaryOutputView.wrapAsByteBuffer();
		temporaryOutputView.clear();
		return result;
	}

	@Override
	protected RecordId setOutRecordId(
			SerializationDelegate<SemiDeserializedStreamRecord> outRecord, RecordId recordId) {

		long sourceRecordId = outRecord.getInstance().getId().getSourceRecordId();
		RecordId newSourceRecordId = RecordId.newRecordId(sourceRecordId);

		outRecord.getInstance().setId(newSourceRecordId);

		return newSourceRecordId;
	}

	@Override
	public void setAnchorRecord(RecordId id) {
		// intentionally left blank
	}

}
