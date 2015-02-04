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

package org.apache.flink.streaming.api.streamvertex;

import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.FTLayerBuilder;
import org.apache.flink.streaming.api.ft.layer.Anchorer;
import org.apache.flink.streaming.api.ft.layer.FT;
import org.apache.flink.streaming.api.ft.layer.NonFT;
import org.apache.flink.streaming.api.ft.layer.Persister;
import org.apache.flink.streaming.api.ft.layer.Xorer;
import org.apache.flink.streaming.api.ft.layer.util.AsSemiDeserializedStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.util.FTAnchorer;
import org.apache.flink.streaming.api.ft.layer.util.FTPersister;
import org.apache.flink.streaming.api.ft.layer.util.SourceFTXorer;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.util.MutableObjectIterator;

import static org.apache.flink.streaming.api.FTLayerBuilder.*;

public class StreamSourceVertex<OUT> extends StreamVertex<OUT, OUT> {

	private SourceInvokable<OUT> sourceInvokable;
	// private StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> ftWriter;

	public StreamSourceVertex() {
		super();
		sourceInvokable = null;
		// ftWriter = null;
	}

	@Override
	public void initializeInvoke() {
		// TODO initialize serializers of ftWriters
	}

	@Override
	public void setInputsOutputs() {
		// TODO set FT RecordWriter, PersistencePartitioner
		if (ftStatus == FTStatus.ON) {
			StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> ftWriter = new
					StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>
					(getNextWriter(), new RoundRobinChannelSelector<SerializationDelegate<StreamRecord<OUT>>>(), 10);
			StreamRecordSerializer<OUT> serializer = configuration
					.getTypeSerializerOut1(userClassLoader);
			AsSemiDeserializedStreamRecordSerializer<OUT> semiDeserializedSerializer = new
					AsSemiDeserializedStreamRecordSerializer<OUT>(serializer);
			Anchorer anchorer = new FTAnchorer();
			Persister<OUT> persister = new FTPersister<OUT>(anchorer, ftWriter,
					semiDeserializedSerializer);
			Xorer sourceXorer = new SourceFTXorer(ftWriter);
			abstractFT = new FT(persister, sourceXorer, anchorer);
		} else {
			abstractFT = new NonFT();
		}
		userInvokable = sourceInvokable;
		outputHandler = new OutputHandler<OUT>(this, abstractFT);
	}

	@Override
	protected void setInvokable() {
		sourceInvokable = configuration.getUserInvokable(userClassLoader);
		sourceInvokable.setup(this, abstractFT);
	}

	@Override
	public void invoke() throws Exception {
		initializeInvoke();
		inputHandler = new InputHandler<OUT>(this);
		outputHandler.invokeUserFunction("SOURCE", sourceInvokable);
		abstractFT.close();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		return null;
	}

}
