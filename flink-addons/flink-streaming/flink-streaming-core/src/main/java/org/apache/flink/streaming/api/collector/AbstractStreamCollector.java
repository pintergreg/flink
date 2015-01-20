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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.ft.context.FTContext;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamvertex.StreamVertex;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector for tuples in Apache Flink stream processing. The collected values
 * will be wrapped with ID in a {@link StreamRecord} and then emitted to the
 * outputs.
 * 
 * @param <T>
 *            Type of the Tuples/Objects collected.
 */
public abstract class AbstractStreamCollector<T, OUT extends IOReadableWritable> implements
		Collector<T> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCollector.class);

	private FTContext ftContext;

	protected List<RecordWriter<OUT>> outputs;
	protected Map<String, List<RecordWriter<OUT>>> outputMap;
	protected RecordId anchorRecord;
	protected TypeSerializer<StreamRecord<T>> outSerializer;

	/**
	 * Creates a new StreamCollector
	 *
	 * @param serializationDelegate
	 *            Serialization delegate used for serialization
	 */

	public AbstractStreamCollector(StreamVertex<?, T> streamComponent, FTContext ftContext) {
		outputs = new ArrayList<RecordWriter<OUT>>();
		StreamConfig configuration = new StreamConfig(streamComponent.getTaskConfiguration());

		this.ftContext = ftContext;

		try {
			outSerializer = configuration.getTypeSerializerOut1(streamComponent.getUserClassLoader());

			int numberOfOutputs = configuration.getNumberOfOutputs();
			long bufferTimeout = configuration.getBufferTimeout();

			for (int outputNumber = 0; outputNumber < numberOfOutputs; outputNumber++) {
				ChannelSelector<OUT> outputPartitioner = null;

				try {
					outputPartitioner = configuration.getPartitioner(
							streamComponent.getUserClassLoader(), outputNumber);

				} catch (Exception e) {
					throw new StreamVertexException("Cannot deserialize partitioner for "
							+ streamComponent.getName() + " with " + outputNumber + " outputs", e);
				}

				RecordWriter<OUT> output;

				if (bufferTimeout >= 0) {
					output = new StreamRecordWriter<OUT>(streamComponent, outputPartitioner,
							bufferTimeout);

					if (LOG.isTraceEnabled()) {
						LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
								bufferTimeout, streamComponent.getClass().getSimpleName());
					}
				} else {
					output = new RecordWriter<OUT>(streamComponent, outputPartitioner);

					if (LOG.isTraceEnabled()) {
						LOG.trace("RecordWriter initiated for {}", streamComponent.getClass()
								.getSimpleName());
					}
				}

				List<String> outputNames = configuration.getOutputName(outputNumber);
				boolean isSelectAllOutput = configuration.getSelectAll(outputNumber);

				addOutput(output, outputNames, isSelectAllOutput);

				if (LOG.isTraceEnabled()) {
					LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner
							.getClass().getSimpleName(), outputNumber, streamComponent.getClass()
							.getSimpleName());
				}
			}

			if (streamComponent.getConfiguration().getDirectedEmit()) {
				OutputSelector<T> outputSelector = streamComponent.getConfiguration()
						.getOutputSelector(streamComponent.getUserClassLoader());
			}
		} catch (StreamVertexException e) {
			throw new StreamVertexException("Cannot register outputs for "
					+ streamComponent.getClass().getSimpleName(), e);
		}

	}

	public AbstractStreamCollector(AbstractStreamCollector<T, OUT> other) {
		this.outSerializer = other.outSerializer;
		this.outputs = other.outputs;
		this.outputMap = other.outputMap;
		this.anchorRecord = other.anchorRecord;
		this.ftContext = other.ftContext;
	}

	/**
	 * Adds an output with the given user defined name
	 * 
	 * @param output
	 *            The RecordWriter object representing the output.
	 * @param outputNames
	 *            User defined names of the output.
	 * @param isSelectAllOutput
	 *            Marks whether all the outputs are selected.
	 */
	protected void addOutput(RecordWriter<OUT> output, List<String> outputNames,
			boolean isSelectAllOutput) {
		addOneOutput(output, outputNames, isSelectAllOutput);
	}

	protected void addOneOutput(RecordWriter<OUT> output, List<String> outputNames,
			boolean isSelectAllOutput) {

		outputs.add(output);
		for (String outputName : outputNames) {
			if (outputName != null) {
				if (!outputMap.containsKey(outputName)) {
					outputMap.put(outputName, new ArrayList<RecordWriter<OUT>>());
					outputMap.get(outputName).add(output);
				} else {
					if (!outputMap.get(outputName).contains(output)) {
						outputMap.get(outputName).add(output);
					}
				}
			}
		}
	}

	public void flushOutputs() throws IOException, InterruptedException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Closing collector outputs");
		}
		for (RecordWriter<OUT> output : outputs) {
			if (output instanceof StreamRecordWriter) {
				((StreamRecordWriter<OUT>) output).close();
			} else {
				output.flush();
			}
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Closed and flushed collector outputs");
		}
	}

	public void initializeOutputSerializers() {
		for (RecordWriter<OUT> output : outputs) {
			output.initializeSerializers();
		}
	}

	/**
	 * Collects and emits a tuple/object to the outputs by reusing a
	 * StreamRecord object.
	 * 
	 * @param outputObject
	 *            Object to be collected and emitted.
	 */
	@Override
	public void collect(T outputObject) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Collected: {}", outputObject);
		}
		OUT outRecord = produceOutRecord(outputObject);
		emit(outRecord);
	}

	protected abstract OUT produceOutRecord(T outObject);

	protected abstract RecordId setOutRecordId(OUT outRecord, RecordId recordId);

	/**
	 * Emits a StreamRecord to the outputs.
	 * 
	 * @param outRecord
	 *            StreamRecord to emit.
	 */
	private void emit(OUT outRecord) {
		emitToOutputs(outRecord);
	}

	protected void emitToOutputs(OUT outRecord) {
		for (RecordWriter<OUT> output : outputs) {
			try {
				emitToOneOutput(outRecord, output);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit failed due to: {}", StringUtils.stringifyException(e));
				}
			}
		}
	}

	/**
	 * Emits a data point to one output, generates an id and sends it to the
	 * AckerTask.
	 * 
	 * @param outRecord
	 * 
	 * @param output
	 *            The output to emit to
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void emitToOneOutput(OUT outRecord, RecordWriter<OUT> output) throws IOException,
			InterruptedException {
		RecordId newRecordId = setOutRecordId(outRecord, anchorRecord);
		output.emit(outRecord);

		ftContext.xor(newRecordId);
	}

	public void setAnchorRecord(RecordId anchorRecord) {
		this.anchorRecord = anchorRecord;
	}

	@Override
	public void close() {
	}
}
