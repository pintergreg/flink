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

package org.apache.flink.streaming.api.ft.layer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.network.serialization.DataInputDeserializer;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.apache.flink.streaming.api.ft.layer.util.AsStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecordSerializer;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.junit.Test;

public class SerializationTest {

	// private byte[] getByteArrayFromOutputView(DataOutputSerializer target) {
	// ByteBuffer buffer = target.wrapAsByteBuffer();
	// byte[] repr = buffer.array();
	// byte[] actual = new byte[buffer.limit()];
	// for (int i = 0; i < buffer.limit(); i++) {
	// actual[i] = repr[i];
	// }
	// return actual;
	// }

	@Test
	public void serializationTest() {
		String sObject;
		long sId;
		RecordId sRecordId;
		SemiDeserializedStreamRecord sSDSRecord;
		ByteBuffer sBuffer;
		SemiDeserializedStreamRecord fSDSRecord;
		RecordId fRecordId;
		ByteBuffer fBuffer;
		StreamRecord<String> mSRecord1;
		StreamRecord<String> mSRecord2;
		RecordId mRecordId1;
		RecordId mRecordId2;
		String mObject1;
		String mObject2;

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
			sObject = "Thry thee bleen!";
			sId = 0x18L;
			sTypeSerializer.serialize(sObject, out);
			sBuffer = out.wrapAsByteBuffer();

			sRecordId = RecordId.newRecordId(sId);
			sSDSRecord = new SemiDeserializedStreamRecord(sBuffer, sObject.hashCode(), sRecordId);
			sSDSRecordSerializer.serialize(sSDSRecord, sOut1);
			aSRecordSerializer.serialize(sSDSRecord, sOut2);

			fIn = new DataInputDeserializer(sOut1.wrapAsByteBuffer());
			fSDSRecord = aSRecordSerializer.deserialize(fIn);
			fRecordId = fSDSRecord.getId();
			fBuffer = fSDSRecord.getSerializedRecord();
			aSRecordSerializer.serialize(fSDSRecord, fOut);

			mIn1 = new DataInputDeserializer(fOut.wrapAsByteBuffer());
			mSRecord1 = mSRecordSerializer.deserialize(mIn1);
			mRecordId1 = mSRecord1.getId();
			mObject1 = mSRecord1.getObject();

			mIn2 = new DataInputDeserializer(sOut2.wrapAsByteBuffer());
			mSRecord2 = mSRecordSerializer.deserialize(mIn2);
			mRecordId2 = mSRecord2.getId();
			mObject2 = mSRecord2.getObject();

			printHeader("At source to ftLayer");
			print();
			printObject("sObject", sObject);
			printRecordId("sRecordId", sRecordId);
			printBuffer("sBuffer", sBuffer);
			printOutputView("sOut1", sOut1);
			print();

			printHeader("At ftLayer to first task");
			print();
			printRecordId("fRecordId", fRecordId);
			printBuffer("fBuffer", fBuffer);
			printOutputView("fOut", fOut);
			print();

			printHeader("At first task from ftLayer");
			print();
			printRecordId("mRecordId1", mRecordId1);
			printObject("mObject1", mObject1);
			print();

			printHeader("At source to first task");
			print();
			printOutputView("sOut2", sOut2);
			print();

			printHeader("At first task from source");
			print();
			printRecordId("mRecordId2", mRecordId2);
			printObject("mObject2", mObject2);
			print();

			out.clear();
			sOut1.clear();
			sOut2.clear();
			fOut.clear();

			sObject = "Zekalif!";
			sId = 0x20L;
			sTypeSerializer.serialize(sObject, out);
			sBuffer = out.wrapAsByteBuffer();

			sRecordId = RecordId.newRecordId(sId);
			sSDSRecord = new SemiDeserializedStreamRecord(sBuffer, sObject.hashCode(), sRecordId);
			sSDSRecordSerializer.serialize(sSDSRecord, sOut1);
			aSRecordSerializer.serialize(sSDSRecord, sOut2);

			fIn = new DataInputDeserializer(sOut1.wrapAsByteBuffer());
			fSDSRecord = aSRecordSerializer.deserialize(fIn);
			fRecordId = fSDSRecord.getId();
			fBuffer = fSDSRecord.getSerializedRecord();
			aSRecordSerializer.serialize(fSDSRecord, fOut);

			mIn1 = new DataInputDeserializer(fOut.wrapAsByteBuffer());
			mSRecord1 = mSRecordSerializer.deserialize(mIn1);
			mRecordId1 = mSRecord1.getId();
			mObject1 = mSRecord1.getObject();

			mIn2 = new DataInputDeserializer(sOut2.wrapAsByteBuffer());
			mSRecord2 = mSRecordSerializer.deserialize(mIn2);
			mRecordId2 = mSRecord2.getId();
			mObject2 = mSRecord2.getObject();

			printHeader("");
			printHeader("At source to ftLayer");
			print();
			printObject("sObject", sObject);
			printRecordId("sRecordId", sRecordId);
			printBuffer("sBuffer", sBuffer);
			printOutputView("sOut1", sOut1);
			print();

			printHeader("At ftLayer to first task");
			print();
			printRecordId("fRecordId", fRecordId);
			printBuffer("fBuffer", fBuffer);
			printOutputView("fOut", fOut);
			print();

			printHeader("At first task from ftLayer");
			print();
			printRecordId("mRecordId1", mRecordId1);
			printObject("mObject1", mObject1);
			print();

			printHeader("At source to first task");
			print();
			printOutputView("sOut2", sOut2);
			print();

			printHeader("At first task from source");
			print();
			printRecordId("mRecordId2", mRecordId2);
			printObject("mObject2", mObject2);
		} catch (IOException e) {
		}
	}

	private String asString(byte[] byteArray, int length) {
		if (length > byteArray.length) {
			System.err.println("Wrong length!");
		}
		String string = "[";
		for (int i = 0; i < length - 1; i++) {
			string += byteArray[i] + ", ";
		}
		string += byteArray[length - 1] + "]";
		return string;
	}

	public String asString(ByteBuffer byteBuffer, int length) {
		byte[] byteArray = byteBuffer.array();
		return asString(byteArray, length);
	}

	public void print(String name, Object object) {
		System.out.println(name + ": " + object);
	}

	public void printHeader(String name) {
		System.out.println("*** " + name + " ***");
	}

	public void print() {
		System.out.println();
	}

	public void printObject(String name, Object object) {
		printHeader(name);
		print("object", object);
		print();
	}

	public void printOutputView(String name, DataOutputSerializer out) {
		ByteBuffer buffer = out.wrapAsByteBuffer();
		int limit = buffer.limit();
		int capacity = buffer.capacity();
		printHeader(name);
		print("position", out.length());
		print("buffer capacity", capacity);
		print("full buffer", asString(buffer, capacity));
		print("buffer limit", limit);
		print("actual buffer", asString(buffer, limit));
		print();
	}

	public void printBuffer(String name, ByteBuffer buffer) {
		int limit = buffer.limit();
		int capacity = buffer.capacity();
		printHeader(name);
		print("capacity", capacity);
		print("full buffer", asString(buffer, capacity));
		print("limit", limit);
		print("actual buffer", asString(buffer, limit));
		print();
	}

	public void printRecordId(String name, RecordId id) {
		printHeader(name);
		print("source record id", id.getSourceRecordId());
		print("record id", id.getRecordId());
		print();
	}

}
