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

import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.RecordReplayer;
import org.apache.flink.streaming.api.ft.layer.collector.FailedRecordCollector;
import org.apache.flink.streaming.api.ft.layer.event.XorEvent;
import org.apache.flink.streaming.api.ft.layer.event.XorEventListener;
import org.apache.flink.streaming.api.ft.layer.partitioner.ReplayPartitioner;
import org.apache.flink.streaming.api.ft.layer.partitioner.ReplayPartitionerFactory;
import org.apache.flink.streaming.api.ft.layer.serialization.AsStreamRecordSerializer;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.util.FTEdgeInformation;
import org.apache.flink.streaming.api.ft.layer.util.SourceReplayer;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.io.MultiBufferReaderBase;
import org.apache.flink.streaming.io.MultiReaderIterator;
import org.apache.flink.streaming.io.MultiRecordReader;
import org.apache.flink.streaming.io.MultiSingleInputReaderIterator;
import org.apache.flink.streaming.io.MultiUnionReaderIterator;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		ftLayer = new FTLayer();
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

		//This list stores information (sourceID, taskID and Partition Strategy)
		//about only every "Source to Task" edges
		List<FTEdgeInformation> edgeInformations = config.getEdgeInformations();
		SourceReplayer[] srcReplayers = new SourceReplayer[numberOfSources];

		streamOutputs = new ArrayList<RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>>(numberOfOutputs);

		//stores FTL to source successive tasks edges in the order of creation,
		// identified by the same ID that is stored in the edgeInformations
		this.outputs = getEnvironment().getAllWriters();

		try {
			inputOutputSerializer = new AsStreamRecordSerializer();

			SerializationDelegate<SemiDeserializedStreamRecord> outSerializationDelegate =
					new SerializationDelegate<SemiDeserializedStreamRecord>(inputOutputSerializer);
			outSerializationDelegate.setInstance(inputOutputSerializer.createInstance());

			bufferTimeout = config.getBufferTimeout();

			//iterate over source to task edges and create Record Writers about the information and
			//stores them according to ths source IDs. To create a RecordWriter a BufferWriter and a
			//PartitionStrategy is needed.
			for (FTEdgeInformation edgeInfo : edgeInformations) {
				LOG.debug("EDGE_INFO", "FROM::{}, TO::{}, WITH A '{}' STRATEGY", edgeInfo.getSourceID(), edgeInfo.getTaskID(),
						edgeInfo.getPartitioningStrategy().name());

				//if there is no SourceReplayer for the given source create one (otherwise just add a RecordWriter)
				if (srcReplayers[edgeInfo.getSourceID()] == null) {
					srcReplayers[edgeInfo.getSourceID()] = new SourceReplayer(edgeInfo.getSourceID(), inputOutputSerializer, ftLayer);
				}
				ReplayPartitioner outputPartitioner = ReplayPartitionerFactory.getReplayPartitioner(edgeInfo.getPartitioningStrategy());

				if (bufferTimeout >= 0) {
					srcReplayers[edgeInfo.getSourceID()].addRecordWriter(
							new StreamRecordWriter<SerializationDelegate
									<SemiDeserializedStreamRecord>>(getEnvironment().getWriter(edgeInfo.getTaskID()),
									outputPartitioner, bufferTimeout)
					);
				} else {
					srcReplayers[edgeInfo.getSourceID()].addRecordWriter(
							new RecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(
									getEnvironment().getWriter(edgeInfo.getTaskID()), outputPartitioner)
					);
				}

			}

		} catch (StreamVertexException e) {
			throw new StreamVertexException("Cannot register outputs for "
					+ this.getClass().getSimpleName(), e);
		}

		recordReplayer = new FTRecordReplayer(srcReplayers);
		ftLayer.setRecordReplayer(recordReplayer);
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

		ftLayer.open();

		reuse = inputOutputSerializer.createInstance();

		int fromInput;

		while ((fromInput = inputIter.nextWithIndex(reuse)) != -1) {
			ftLayer.newSourceRecord(reuse, fromInput);
		}

		ftLayer.close();

		flushOutputs();

		if (LOG.isTraceEnabled()) {
			LOG.trace("Invoke finished:\tFTLayerVertex");
		}
	}

}