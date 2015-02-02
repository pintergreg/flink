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

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.Anchorer;
import org.apache.flink.streaming.api.ft.layer.Persister;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;

public class FTPersister<T> implements Persister<T> {
	private Anchorer anchorer;
	private StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> recordWriter;
	private SerializationDelegate<StreamRecord<T>> serializationDelegate;

	public FTPersister(Anchorer anchorer,
			StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> recordWriter,
			StreamRecordSerializer<T> serializer) {
		this.anchorer = anchorer;
		this.recordWriter = recordWriter;
		this.serializationDelegate = new SerializationDelegate<StreamRecord<T>>(
				serializer);
	}

	@Override
	public void persist(StreamRecord<T> record) {
		record.setId(RecordId.newSourceRecordId());
		anchorer.setAnchorRecord(record);
		serializationDelegate.setInstance(record);
		try {
			recordWriter.emit(serializationDelegate);
		} catch (Exception e) {
			// TODO log or throw exception
			e.printStackTrace();
		}
	}
}
