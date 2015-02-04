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

package org.apache.flink.streaming.api.ft.layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.reader.UnionBufferReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.collector.FailedRecordCollector;
import org.apache.flink.streaming.api.ft.layer.event.XorEvent;
import org.apache.flink.streaming.api.ft.layer.event.XorEventListener;
import org.apache.flink.streaming.api.ft.layer.util.AsStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.util.EmptyIOReadableWritable;
import org.apache.flink.streaming.api.ft.layer.util.FTLayerConfig;
import org.apache.flink.streaming.api.ft.layer.util.FTRecordReplayer;
import org.apache.flink.streaming.api.ft.layer.util.ReaderInitializerEvent;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.io.MultiBufferReaderBase;
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
	protected static int numOfTasks = 0;

	protected FTLayerConfig config;
	protected int instanceID;
	protected SemiDeserializedStreamRecord reuse;

	// inputs
	private int numberOfSources;
	private ReaderBase inputs;
	private MultiReaderIterator<SemiDeserializedStreamRecord> inputIter;

	// outputs
	private int numberOfOutputs;
	private BufferWriter[] outputs;
	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> streamOutputs;
	private long bufferTimeout;
	private AsStreamRecordSerializer inputOutputSerializer;

	private FailedRecordCollector[] failedRecordCollectors;

	// logic
	protected FTLayer ftLayer;
	private RecordReplayer recordReplayer;

	public FTLayerVertex() {
		instanceID = numOfTasks;
		ftLayer = new FTLayer(recordReplayer);
		numOfTasks++;
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

		inputs.subscribeToTaskEvent(xorEventListener, XorEvent.class);

		for (BufferWriter output : outputs) {
			output.subscribeToEvent(xorEventListener, XorEvent.class);
		}

	}

	// output handling

	private void registerOutputs() {

		ArrayList<ArrayList<Integer>> sourceSuccessives = config.getSourceSuccessives();
		streamOutputs = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>(numberOfOutputs);

		this.outputs = getEnvironment().getAllWriters();

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

					output = new StreamRecordWriter<SerializationDelegate
							<SemiDeserializedStreamRecord>>(getEnvironment().getWriter(outputNumber)
							, outputPartitioner,
							bufferTimeout);

					if (LOG.isTraceEnabled()) {
						LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
								bufferTimeout, getClass().getSimpleName());
					}
				} else {
					output = new RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(
							getEnvironment().getWriter(outputNumber), outputPartitioner);

					if (LOG.isTraceEnabled()) {
						LOG.trace("RecordWriter initiated for {}", getClass().getSimpleName());
					}
				}
				streamOutputs.add(output);

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
		recordReplayer = new FTRecordReplayer(failedRecordCollectors);
	}


	private List<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> getSourceSuccessives(
			int sourceIndex, ArrayList<ArrayList<Integer>> sourceSuccessives) {

		ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>> sourceSuccessive = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>(
				sourceSuccessives.get(sourceIndex).size());

		for (int i = 0; i < sourceSuccessives.get(sourceIndex).size(); i++) {
			sourceSuccessive.add(streamOutputs.get(sourceSuccessives.get(sourceIndex).get(i) - 1));
		}

		return sourceSuccessive;
	}

	private void flushOutputs() throws IOException, InterruptedException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("closeflushing FTLayer outputs");
		}
		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output :
				streamOutputs) {
			if (output instanceof StreamRecordWriter) {
				((StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>) output)
						.close();
			} else {
				output.flush();
			}
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("closeflushed FTLayer outputs");
		}
	}

	private void initializeOutputSerializers() {
		for (RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> output :
				streamOutputs) {
			// TODO output.initializeSerializers();
		}
	}

	// input handling

	private void registerInputs() {

		inputOutputSerializer = new AsStreamRecordSerializer();

		numberOfOutputs = config.getNumberOfOutputs();
		numberOfSources = config.getNumberOfSources();

		if (numberOfSources > 0) {
			if (numberOfSources < 2) {

				MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>> mutableRecordReader = new MutableRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>(this.getEnvironment()
						.getReader(0));
				inputs = mutableRecordReader;

				inputIter = new MultiSingleInputReaderIterator<SemiDeserializedStreamRecord>(mutableRecordReader, inputOutputSerializer);
			} else {

				MultiBufferReaderBase multiReader = new MultiBufferReaderBase(this.getEnvironment().getAllReaders());

				MultiRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>> multiRecordReader = new MultiRecordReader<DeserializationDelegate<SemiDeserializedStreamRecord>>(multiReader);
				inputs = multiRecordReader;

				inputIter = new MultiUnionReaderIterator<SemiDeserializedStreamRecord>(multiRecordReader, inputOutputSerializer);
			}
		}
	}

	public int getInstanceID() {
		return instanceID;
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Invoked:\tFTLayerVertex");
		}

		initializeOutputSerializers();

		initializeOutConnections();

		reuse = inputOutputSerializer.createInstance();

		int fromInput;

		while ((fromInput = inputIter.nextWithIndex(reuse)) != -1) {
			ftLayer.newSourceRecord(reuse, fromInput);
		}

		while (!ftLayer.isEmpty()) {
			Thread.sleep(500L);
		}

		flushOutputs();
		if (LOG.isTraceEnabled()) {
			LOG.trace("Invoke finished:\tFTLayerVertex");
		}
	}


	private void initializeOutConnections() {
		for (BufferWriter output : outputs) {
			RecordWriter<IOReadableWritable> writer = new RecordWriter<IOReadableWritable>(output);
			try {
				writer.emit(new EmptyIOReadableWritable());
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


}