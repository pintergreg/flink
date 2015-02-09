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

package org.apache.flink.streaming.api.ft;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.ft.layer.serialization.AsStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecordSerializer;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

	@Test
	public void serializationTest() {
		String sObject;
		long sId;
		RecordId sRecordId;
		int sHashCode;
		SemiDeserializedStreamRecord sSDSRecord;
		ByteBuffer sBuffer;
		SemiDeserializedStreamRecord fSDSRecord;
		int fHashCode;
		RecordId fRecordId;
		StreamRecord<String> mSRecord1;
		StreamRecord<String> mSRecord2;
		String mObject1;
		String mObject2;
		RecordId mRecordId1;
		RecordId mRecordId2;

		TypeInformation<String> typeInfo = TypeExtractor.getForObject("");
		TypeSerializer<String> sTypeSerializer = typeInfo.createSerializer();
		SemiDeserializedStreamRecordSerializer sSDSRecordSerializer = new SemiDeserializedStreamRecordSerializer();
		AsStreamRecordSerializer aSRecordSerializer = new AsStreamRecordSerializer();
		StreamRecordSerializer<String> mSRecordSerializer = new StreamRecordSerializer<String>(
				typeInfo);

		DataOutputSerializer out = new DataOutputSerializer(20);
		DataOutputSerializer sOut1 = new DataOutputSerializer(50);
		DataOutputSerializer sOut2 = new DataOutputSerializer(40);
		DataInputView fIn;
		DataOutputSerializer fOut = new DataOutputSerializer(40);
		DataInputView mIn1;
		DataInputView mIn2;

		try {
			sObject = "This is a message.";
			sId = 0x18L;
			sTypeSerializer.serialize(sObject, out);
			sBuffer = out.wrapAsByteBuffer();

			sRecordId = RecordId.newRecordId(sId);
			sHashCode = sObject.hashCode();
			sSDSRecord = new SemiDeserializedStreamRecord(sBuffer, sHashCode, sRecordId);
			sSDSRecordSerializer.serialize(sSDSRecord, sOut1);
			aSRecordSerializer.serialize(sSDSRecord, sOut2);

			fIn = new DataInputDeserializer(sOut1.wrapAsByteBuffer());
			fSDSRecord = aSRecordSerializer.deserialize(fIn);
			fRecordId = fSDSRecord.getId();
			fHashCode = fSDSRecord.getHashCode();
			aSRecordSerializer.serialize(fSDSRecord, fOut);

			mIn1 = new DataInputDeserializer(fOut.wrapAsByteBuffer());
			mSRecord1 = mSRecordSerializer.deserialize(mIn1);
			mRecordId1 = mSRecord1.getId();
			mObject1 = mSRecord1.getObject();

			mIn2 = new DataInputDeserializer(sOut2.wrapAsByteBuffer());
			mSRecord2 = mSRecordSerializer.deserialize(mIn2);
			mRecordId2 = mSRecord2.getId();
			mObject2 = mSRecord2.getObject();

			assertEquals(fRecordId, sRecordId);
			assertEquals(fHashCode, sHashCode);
			assertEquals(mRecordId1, sRecordId);
			assertEquals(mObject1, sObject);
			assertEquals(mRecordId2, sRecordId);
			assertEquals(mObject2, sObject);

			out.clear();
			sOut1.clear();
			sOut2.clear();
			fOut.clear();

			sObject = "message2";
			sId = 0x20L;
			sTypeSerializer.serialize(sObject, out);
			sBuffer = out.wrapAsByteBuffer();

			sRecordId = RecordId.newRecordId(sId);
			sHashCode = sObject.hashCode();
			sSDSRecord = new SemiDeserializedStreamRecord(sBuffer, sObject.hashCode(), sRecordId);
			sSDSRecordSerializer.serialize(sSDSRecord, sOut1);
			aSRecordSerializer.serialize(sSDSRecord, sOut2);

			fIn = new DataInputDeserializer(sOut1.wrapAsByteBuffer());
			fSDSRecord = aSRecordSerializer.deserialize(fIn);
			fRecordId = fSDSRecord.getId();
			fHashCode = fSDSRecord.getHashCode();
			aSRecordSerializer.serialize(fSDSRecord, fOut);

			mIn1 = new DataInputDeserializer(fOut.wrapAsByteBuffer());
			mSRecord1 = mSRecordSerializer.deserialize(mIn1);
			mRecordId1 = mSRecord1.getId();
			mObject1 = mSRecord1.getObject();

			mIn2 = new DataInputDeserializer(sOut2.wrapAsByteBuffer());
			mSRecord2 = mSRecordSerializer.deserialize(mIn2);
			mRecordId2 = mSRecord2.getId();
			mObject2 = mSRecord2.getObject();

			assertEquals(fRecordId, sRecordId);
			assertEquals(fHashCode, sHashCode);
			assertEquals(mRecordId1, sRecordId);
			assertEquals(mObject1, sObject);
			assertEquals(mRecordId2, sRecordId);
			assertEquals(mObject2, sObject);
		} catch (IOException e) {
		}
	}
}
