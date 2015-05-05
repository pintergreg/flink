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

import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.runtime.AbstractFTHandler;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StreamOutput<OUT> implements Collector<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamOutput.class);

	private RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;
	private SerializationDelegate<StreamRecord<OUT>> serializationDelegate;
	private StreamRecord<OUT> streamRecord;
	// ex-channelID
	private int instanceID;
	private AbstractFTHandler<?> abstractFTHandler;
	//private int childRecordCounter;

	private String vertexType;

	public StreamOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			int instanceID, SerializationDelegate<StreamRecord<OUT>> serializationDelegate,
			AbstractFTHandler<?> abstractFTHandler, String vertexType) {

		this(output, instanceID, serializationDelegate, abstractFTHandler);

		this.vertexType = vertexType;
	}

	public StreamOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			int instanceID, SerializationDelegate<StreamRecord<OUT>> serializationDelegate,
			AbstractFTHandler<?> abstractFTHandler) {

		this.serializationDelegate = serializationDelegate;
		this.abstractFTHandler = abstractFTHandler;

		if (serializationDelegate != null) {
			this.streamRecord = serializationDelegate.getInstance();
		} else {
			throw new RuntimeException("Serializer cannot be null");
		}
		this.instanceID = instanceID;
		this.output = output;

		// Set zero the child record counter
		//this.childRecordCounter = 0;

	}

	public RecordWriter<SerializationDelegate<StreamRecord<OUT>>> getRecordWriter() {
		return output;
	}

	@Override
	public void collect(OUT record) {
		streamRecord.setObject(record);
		//streamRecord.newId(channelID);
		serializationDelegate.setInstance(streamRecord);

		try {
			/*
			 * Give parameters for deterministic ID generation.
			 * Three parameters are necessary:
			 *  1. instance IDE
			 *  2. parent record ID
			 *  3. child record counter, that is an ordering of egress records
			 *  Instance ID and the child record counter can be given here, parent record ID is passed elsewhere (in FTAnchorHandler)
			 */
			//this.childRecordCounter++;
			//this.childRecordCounter=1;
			if (this.vertexType!=null && this.vertexType.equals("SOURCE")) {
				abstractFTHandler.setOutRecordId(serializationDelegate, this.instanceID, /*1,*/ true);
			}else{
				abstractFTHandler.setOutRecordId(serializationDelegate, this.instanceID, /*this.childRecordCounter,*/ false);
			}

			output.emit(serializationDelegate);
			abstractFTHandler.xor(serializationDelegate.getInstance());
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Emit failed due to: {}", StringUtils.stringifyException(e));
			}
		}
	}

	public void flush() {
		try {
			output.flush();
		} catch (IOException e) {
			throw new RuntimeException("Cannot flush output", e);
		}
	}

	@Override
	public void close() {
		if (output instanceof StreamRecordWriter) {
			((StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>) output).close();
		} else {
			flush();
		}
	}

//	public void resetChildRecordCounter() {
//		this.childRecordCounter = 0;
//	}

}
