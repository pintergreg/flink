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

package org.apache.flink.streaming.api.ft.layer.runtime;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;

public class FTPersister<T> implements Persister<T> {
	private AnchorHandler anchorHandler;
	private StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> recordWriter;
	private SerializationDelegate<StreamRecord<T>> serializationDelegate;

	public FTPersister(AnchorHandler anchorHandler,
			StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> recordWriter,
			StreamRecordSerializer<T> serializer) {
		this.anchorHandler = anchorHandler;
		this.recordWriter = recordWriter;
		this.serializationDelegate = new SerializationDelegate<StreamRecord<T>>(
				serializer);
	}

	@Override
	public void persist(StreamRecord<T> record) {
		//TODO ###ID_GEN -- hát te mi vagy? >> source to FTL communication for persist
		// régi:
		//record.setId(RecordId.newSourceRecordId());
		record.setId(RecordId.newRootId());

		anchorHandler.setAnchorRecord(record);
		serializationDelegate.setInstance(record);
		try {
			recordWriter.emit(serializationDelegate);
		} catch (Exception e) {
			throw new RuntimeException("Cannot emit record to persistence layer", e);
		}
	}

	@Override
	public void close() {
		recordWriter.close();
	}
}
