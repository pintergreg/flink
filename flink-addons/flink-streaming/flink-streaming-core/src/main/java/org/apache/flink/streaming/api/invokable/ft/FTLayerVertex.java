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

package org.apache.flink.streaming.api.invokable.ft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.ft.FailedRecordCollector;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.util.AsStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.util.FTLayerConfig;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.io.MultiReaderIterator;
import org.apache.flink.streaming.io.MultiRecordReader;
import org.apache.flink.streaming.io.MultiSingleInputReaderIterator;
import org.apache.flink.streaming.io.MultiUnionReaderIterator;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.FailedRecordPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTLayerVertex extends AbstractInvokable {

	private static final Logger LOG = LoggerFactory.getLogger(FTLayerVertex.class);

	private static final long WAIT_TIME_FOR_FINISH_IN_MILLIS = 1000L;

	protected static int numOfTasks = 0;

	protected FTLayerConfig config;
	protected int instanceID;
	protected SemiDeserializedStreamRecord reuse;
	protected boolean ftLayerVertexFinished;

	// inputs
	private int numberOfSources;
	private AbstractRecordReader inputs;
	private MultiReaderIterator<SemiDeserializedStreamRecord> inputIter;

	// outputs
	private int numberOfOutputs;
	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> outputs;
	private long bufferTimeout;
	private AsStreamRecordSerializer inputOutputSerializer;

	private FailedRecordCollector[] failedRecordCollectors;

	// logic
	protected FTLayer ftLayer;

	public FTLayerVertex() {
		instanceID = numOfTasks;
		ftLayer = new FTLayer(this);
		numOfTasks++;
		ftLayerVertexFinished = false;
	}

	@Override
	public void registerInputOutput() {

		this.config = new FTLayerConfig(getTaskConfiguration());

		numberOfOutputs = config.getNumberOfOutputs();
		numberOfSources = config.getNumberOfSources();

		registerInputs();
		registerOutputs();

		registerListeners();

		if (LOG.isTraceEnabled()) {
			LOG.trace("Registered input and output:\tFTLayer");
		}
	}

	private void registerListeners() {

		XorEventListener xorEventListener = new XorEventListener(ftLayer);

		inputs.subscribeToEvent(xorEventListener, XorEvent.class);

		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output : outputs) {
			output.subscribeToEvent(xorEventListener, XorEvent.class);
		}

	}

	// output handling

	private void registerOutputs() {

		int[][] sourceSuccessives = config.getSourceSuccesives();

		this.outputs = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>(
				numberOfOutputs);

		try {
			inputOutputSerializer = new AsStreamRecordSerializer();

			SerializationDelegate<SemiDeserializedStreamRecord> outSerializationDelegate = new SerializationDelegate<SemiDeserializedStreamRecord>(
					inputOutputSerializer);
			outSerializationDelegate.setInstance(inputOutputSerializer.createInstance());

			bufferTimeout = config.getBufferTimeout();

			for (int outputNumber = 0; outputNumber < numberOfOutputs; outputNumber++) {

				// TODO inherit sources' partitioners
				FailedRecordPartitioner outputPartitioner = new FailedRecordPartitioner();

				RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output;

				if (bufferTimeout >= 0) {
					output = new StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(
							this, outputPartitioner, bufferTimeout);

					if (LOG.isTraceEnabled()) {
						LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
								bufferTimeout, this.getClass().getSimpleName());
					}
				} else {
					output = new RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(
							this, outputPartitioner);

					if (LOG.isTraceEnabled()) {
						LOG.trace("RecordWriter initiated for {}", this.getClass().getSimpleName());
					}
				}

				outputs.add(output);

				if (LOG.isTraceEnabled()) {
					LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner
							.getClass().getSimpleName(), outputNumber, this.getClass()
							.getSimpleName());
				}
			}
		} catch (StreamVertexException e) {
			throw new StreamVertexException("Cannot register outputs for "
					+ this.getClass().getSimpleName(), e);
		}

		failedRecordCollectors = new FailedRecordCollector[numberOfSources];
		for (int sourceNumber = 0; sourceNumber < numberOfSources; sourceNumber++) {
			failedRecordCollectors[sourceNumber] = new FailedRecordCollector(getSourceSuccessives(
					sourceNumber, sourceSuccessives), inputOutputSerializer, ftLayer);
		}
		System.out.println();
	}
	
	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> getSourceSuccessives(
			int sourceIndex, int[][] sourceSuccessives) {

		ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> sourceSuccessive = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>(
				sourceSuccessives[sourceIndex].length);

		for (int i = 0; i < sourceSuccessives[sourceIndex].length; i++) {
			sourceSuccessive.add(outputs.get(sourceSuccessives[sourceIndex][i]));
		}

		return sourceSuccessive;
	}

	private void flushOutputs() throws IOException, InterruptedException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Closing and flushing FTLayer outputs");
		}

		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output : outputs) {
			if (output instanceof StreamRecordWriter) {
				((StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>) output)
						.close();
			} else {
				output.flush();
			}
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Closed and flushed FTLayer outputs");
		}
	}

	private void initializeOutputSerializers() {
		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output : outputs) {
			output.initializeSerializers();
		}
	}

	// input handling

	private void registerInputs() {

		inputOutputSerializer = new AsStreamRecordSerializer();

		numberOfOutputs = config.getNumberOfOutputs();
		numberOfSources = config.getNumberOfSources();

		if (numberOfSources < 2) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Using single input iterator at FTLayerVertex");
			}

			MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>> input = new MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>(
					this);

			inputs = input;

			inputIter = new MultiSingleInputReaderIterator<SemiDeserializedStreamRecord>(input,
					inputOutputSerializer);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Using union input iterator at FTLayerVertex");
			}

			MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>[] recordReaders = (MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>[]) new MutableRecordReader<?>[numberOfSources];
			for (int i = 0; i < numberOfSources; i++) {
				recordReaders[i] = new MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>(
						this);
			}

			MultiRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>> input = new MultiRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>(
					recordReaders);

			inputs = input;

			inputIter = new MultiUnionReaderIterator<SemiDeserializedStreamRecord>(input,
					inputOutputSerializer);
		}
	}

	public int getInstanceID() {
		return instanceID;
	}

	public void replayRecord(int sourceId, SemiDeserializedStreamRecord record) {
		failedRecordCollectors[sourceId].collect(record);
	}

	public void checkIfFinished() {
		ftLayerVertexFinished = ftLayer.isEmpty();
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Invoked:\tFTLayerVertex");
		}

		initializeOutputSerializers();

		reuse = inputOutputSerializer.createInstance();

		int fromInput;

		while ((fromInput = inputIter.next(reuse)) != -1) {
			ftLayer.newSourceRecord(reuse, fromInput);
		}

		while (!ftLayerVertexFinished) {
			Thread.sleep(WAIT_TIME_FOR_FINISH_IN_MILLIS);
			checkIfFinished();
		}

		flushOutputs();

		if (LOG.isTraceEnabled()) {
			LOG.trace("Invoke finished:\tFTLayerVertex");
		}
	}
}
