/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AsSemiDeserializedStreamRecordSerializer<T> extends StreamRecordSerializer<T> {
	protected TypeSerializer<ByteBuffer> byteBufferSerializer;
	protected DataOutputSerializer temporaryOutputView;

	public AsSemiDeserializedStreamRecordSerializer(StreamRecordSerializer<T> serializer) {
		super(serializer);
		this.byteBufferSerializer = new ByteBufferSerializer();
		this.temporaryOutputView = new DataOutputSerializer(serializer.getLength() < 0 ? 64
				: serializer.getLength());
	}

	@Override
	public void serialize(StreamRecord<T> record, DataOutputView target)
			throws IOException {
		record.getId().write(target);
		target.writeInt(0);
		byteBufferSerializer.serialize(getSerializedObject(record.getObject()), target);
	}

	private ByteBuffer getSerializedObject(T object) {
		try {
			temporaryOutputView.clear();
			typeSerializer.serialize(object, temporaryOutputView);
		} catch (IOException e) {
		}
		return temporaryOutputView.wrapAsByteBuffer();
	}
}
