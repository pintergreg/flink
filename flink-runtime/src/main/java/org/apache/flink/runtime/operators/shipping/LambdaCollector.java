/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.shipping;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.streamrecord.StreamRecord;
import org.apache.flink.api.java.typeutils.streamrecord.StreamRecordSerializer;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class LambdaCollector<T> extends OutputCollector<T> {

	protected RecordWriter<SerializationDelegate<StreamRecord<T>>>[] lambdaWriters;
	private SerializationDelegate<StreamRecord<T>> lambdaDelegate;
	private StreamRecord<T> streamRecord;

	public LambdaCollector(List<RecordWriter<SerializationDelegate<T>>> writers,
			List<RecordWriter<SerializationDelegate<StreamRecord<T>>>> lambdaWriters,
			TypeSerializer<T> serializer, Class<T> dataType) {

		super(writers, serializer);
		this.lambdaWriters = lambdaWriters.toArray(new RecordWriter[lambdaWriters.size()]);

		TypeInformation<T> typeInfo = TypeExtractor.getForClass(dataType);
		StreamRecordSerializer<T> recordSerializer = new StreamRecordSerializer<T>(typeInfo);
		this.lambdaDelegate = new SerializationDelegate<StreamRecord<T>>(recordSerializer);
		this.streamRecord = recordSerializer.createInstance();
	}

	@Override
	public void collect(T record) {
		super.collect(record);
		streamRecord.setObject(record);
		streamRecord.newId(0);
		lambdaDelegate.setInstance(streamRecord);

		for (RecordWriter<SerializationDelegate<StreamRecord<T>>> output : lambdaWriters) {
			try {
				output.emit(lambdaDelegate);
				//TODO: consider streamrecordwriter
				output.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
