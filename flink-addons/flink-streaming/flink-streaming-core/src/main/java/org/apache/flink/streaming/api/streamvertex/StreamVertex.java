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

import java.io.IOException;
import java.util.Map;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.FTLayerBuilder.FTStatus;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.ft.layer.AbstractFT;
import org.apache.flink.streaming.api.ft.layer.Anchorer;
import org.apache.flink.streaming.api.ft.layer.FT;
import org.apache.flink.streaming.api.ft.layer.NonFT;
import org.apache.flink.streaming.api.ft.layer.Xorer;
import org.apache.flink.streaming.api.ft.layer.util.EmptyIOReadableWritable;
import org.apache.flink.streaming.api.ft.layer.util.FTAnchorer;
import org.apache.flink.streaming.api.ft.layer.util.NonFTPersister;
import org.apache.flink.streaming.api.ft.layer.util.ReaderInitializerEvent;
import org.apache.flink.streaming.api.ft.layer.util.ReaderInitializerEventListener;
import org.apache.flink.streaming.api.ft.layer.util.TaskFTXorer;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class StreamVertex<IN, OUT> extends AbstractInvokable implements
		StreamTaskContext<OUT> {

	protected static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	protected static int numVertices = 0;

	protected InputHandler<IN> inputHandler;
	protected OutputHandler<OUT> outputHandler;
	protected StreamInvokable<IN, OUT> userInvokable;

	protected StreamingRuntimeContext context;
	protected Map<String, OperatorState<?>> states;

	protected ClassLoader userClassLoader;

	protected AbstractFT<OUT> abstractFT;
	protected static FTStatus ftStatus;
	private ReaderInitializerEventListener readerInitializer;

	public StreamVertex() {
		userInvokable = null;
		numTasks = newVertex();
		instanceID = numTasks;
	}

	protected static int newVertex() {
		numVertices++;
		return numVertices;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();
		setInvokable();
		registerListeners();
	}

	protected void initialize() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.states = configuration.getOperatorStates(userClassLoader);
		this.context = createRuntimeContext(getEnvironment().getTaskName(), this.states);
		this.ftStatus = configuration.getFTStatus(userClassLoader);
	}

	public void initializeInvoke() {
		// TODO initialize serializers of ftWriters
	}

	protected <T> void invokeUserFunction(StreamInvokable<?, T> userInvokable) throws Exception {
		userInvokable.setRuntimeContext(context);
		userInvokable.open(getTaskConfiguration());

		for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
			invokable.setRuntimeContext(context);
			invokable.open(getTaskConfiguration());
		}

		userInvokable.invoke();
		userInvokable.close();

		for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
			invokable.close();
		}

	}

	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);

		if(ftStatus == FTStatus.ON){
			MutableRecordReader ftReader = new MutableRecordReader(getEnvironment().getReader(0));
			Anchorer anchorer = new FTAnchorer();
			Xorer taskXorer = new TaskFTXorer(ftReader);
			abstractFT = new FT(new NonFTPersister<OUT>(), taskXorer, anchorer);
		} else {
			abstractFT = new NonFT();
		}
		outputHandler = new OutputHandler<OUT>(this, abstractFT);
	}

	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable(userClassLoader);
		userInvokable.setup(this, abstractFT);
	}

	public String getName() {
		return getEnvironment().getTaskName();
	}

	public int getInstanceID() {
		return instanceID;
	}

	public StreamingRuntimeContext createRuntimeContext(String taskName,
			Map<String, OperatorState<?>> states) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(taskName, env, getUserCodeClassLoader(), states);
	}

	@Override
	public void invoke() throws Exception {
		initializeInvoke();
		waitForFTConnection();
		outputHandler.invokeUserFunction("TASK", userInvokable);
//		while(true){
//			System.out.println("Waiting in StreamVertex");
//			Thread.sleep(500);
//		}
	}

	private void registerListeners() {
		if (ftStatus == FTStatus.ON && !(this instanceof StreamSourceVertex)) {
			readerInitializer = new ReaderInitializerEventListener();

			BufferReader ftReader = getEnvironment().getReader(0);
			ftReader.subscribeToTaskEvent(readerInitializer, ReaderInitializerEvent.class);
		}
	}

	private void waitForFTConnection() {
		if (ftStatus == FTStatus.ON && !(this instanceof StreamSourceVertex)) {
			BufferReader ftReaderBuffer = getEnvironment().getReader(0);
			MutableRecordReader<IOReadableWritable> ftReader = new MutableRecordReader<IOReadableWritable>(ftReaderBuffer);

			IOReadableWritable ioReadableWritable = new EmptyIOReadableWritable();

			try {
				ftReader.next(ioReadableWritable);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public StreamConfig getConfig() {
		return configuration;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		if (index == 0) {
			return (MutableObjectIterator<X>) inputHandler.getInputIter();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		if (index == 0) {
			return (StreamRecordSerializer<X>) inputHandler.getInputSerializer();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@Override
	public Collector<OUT> getOutputCollector() {
		return outputHandler.getCollector();
	}

	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		throw new IllegalArgumentException("CoReader not available");
	}

	@Override
	public AbstractFT<OUT> getFT() {
		return abstractFT;
	}


	int currentWriterIndex = 0;
	public BufferWriter getNextWriter() {
		return getEnvironment().getWriter(currentWriterIndex++);
	}
}
