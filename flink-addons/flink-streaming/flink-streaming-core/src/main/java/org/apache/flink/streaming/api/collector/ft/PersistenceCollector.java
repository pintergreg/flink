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

package org.apache.flink.streaming.api.collector.ft;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecordSerializer;
import org.apache.flink.util.Collector;

public class PersistenceCollector implements Collector<SemiDeserializedStreamRecord> {

	private RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output;
	private SerializationDelegate<SemiDeserializedStreamRecord> serializationDelegate;

	public PersistenceCollector(
			RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output) {

		this.output = output;
		TypeSerializer<SemiDeserializedStreamRecord> serializer = new SemiDeserializedStreamRecordSerializer();
		this.serializationDelegate = new SerializationDelegate<SemiDeserializedStreamRecord>(
				serializer);
	}

	@Override
	public void collect(SemiDeserializedStreamRecord record) {
		serializationDelegate.setInstance(record);
		try {
			output.emit(serializationDelegate);
		} catch (Exception e) {
			// TODO log or throw exception
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
	}

}
