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

import java.util.ArrayList;

import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.io.CoRecordReader;

public class CoInputHandler<IN1, IN2> {
	private StreamRecordSerializer<IN1> inputDeserializer1 = null;
	private StreamRecordSerializer<IN2> inputDeserializer2 = null;

//	private MutableObjectIterator<StreamRecord<IN1>> inputIter1;
//	private MutableObjectIterator<StreamRecord<IN2>> inputIter2;

	private CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>> coReader;
	private CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> coIter;
	private MutableRecordReader<DeserializationDelegate<StreamRecord<Object>>> ftInput;

	private CoStreamVertex<IN1, IN2, ?> coStreamVertex;
	private StreamConfig configuration;

	public CoInputHandler(CoStreamVertex<IN1, IN2, ?> streamComponent) {
		this.coStreamVertex = streamComponent;
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());
		try {
			setConfigInputs();
		} catch (Exception e) {
			throw new StreamVertexException("Cannot register inputs for "
					+ getClass().getSimpleName(), e);
		}

	}

	protected void setConfigInputs() throws StreamVertexException {

		setDeserializers();

		int numberOfInputs = configuration.getNumberOfInputs();
		ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>> inputList1 = new ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>>();
		ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>> inputList2 = new ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>>();

		ftInput = new MutableRecordReader<DeserializationDelegate<StreamRecord<Object>>>(
				coStreamVertex);
		
		for (int i = 1; i < numberOfInputs; i++) {
			int inputType = configuration.getInputType(i);
			System.out.println("#inputType: " + inputType);
			switch (inputType) {
			case 1:
				inputList1.add(new MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>(
						coStreamVertex));
				break;
			case 2:
				inputList2.add(new MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>(
						coStreamVertex));
				break;
			default:
				throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		coReader = new CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>>(
				inputList1, inputList2);
		coIter = createInputIterator();

	}

	private void setDeserializers() {
//		TypeInformation<IN1> inputTypeInfo1 = configuration.getTypeInfoIn1(coStreamVertex
//				.getUserClassLoader());
//		if (inputTypeInfo1 != null) {
//			inputDeserializer1 = new StreamRecordSerializer<IN1>(inputTypeInfo1);
//		}
//
//		TypeInformation<IN2> inputTypeInfo2 = configuration.getTypeInfoIn2(coStreamVertex
//				.getUserClassLoader());
//		if (inputTypeInfo2 != null) {
//			inputDeserializer2 = new StreamRecordSerializer<IN2>(inputTypeInfo2);
//		}

		inputDeserializer1 = configuration.getTypeSerializerIn1(coStreamVertex.getUserClassLoader());
		inputDeserializer2 = configuration.getTypeSerializerIn2(coStreamVertex.getUserClassLoader());

	}

	private CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> createInputIterator() {
		final CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> iter = new CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>>(
				coReader, inputDeserializer1, inputDeserializer2);
		return iter;
	}

	public StreamRecordSerializer<IN1> getInputDeserializer1() {
		return inputDeserializer1;
	}

	public StreamRecordSerializer<IN2> getInputDeserializer2() {
		return inputDeserializer2;
	}

	public CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> getCoInputIter() {
		return coIter;
	}

	public MutableRecordReader<DeserializationDelegate<StreamRecord<Object>>> getPersistanceInput() {
		return ftInput;
	}
}
