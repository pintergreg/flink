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

import java.util.Map;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.AbstractStreamCollector;
import org.apache.flink.streaming.api.collector.ft.AckerCollector;
import org.apache.flink.streaming.api.ft.context.FTContext;
import org.apache.flink.streaming.api.ft.context.NoFTContext;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.state.OperatorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamVertex<IN, OUT> extends AbstractInvokable implements StreamTaskContext<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(StreamVertex.class);

	private static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	protected String name;
	private static int numVertices = 0;

	protected String functionName;

	protected AbstractStreamCollector<OUT, ?> collector;

	private StreamingRuntimeContext context;
	private Map<String, OperatorState<?>> states;

	protected ClassLoader userClassLoader;

	protected AckerCollector ackerCollector;
	protected FTContext ftContext;

	public StreamVertex() {
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

		if (LOG.isDebugEnabled()) {
			LOG.debug("Registered input and output:\t{}", name);
		}
	}

	protected void initialize() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.name = configuration.getVertexName();
		this.functionName = configuration.getFunctionName();
		this.states = configuration.getOperatorStates(userClassLoader);
		this.context = createRuntimeContext(name, this.states);
	}

	protected abstract void setInputsOutputs();

	protected abstract void setInvokable();

	protected abstract StreamInvokable<IN, OUT> getInvokable();

	protected void invokeUserFunction() throws Exception {
		StreamInvokable<IN, OUT> invokable = getInvokable();

		invokable.setRuntimeContext(context);
		invokable.open(getTaskConfiguration());
		invokable.invoke();
		invokable.close();
	}

	protected void setFaultToleranceContext() {
		if (configuration.getFaultToleranceTurnedOnFlag()) {
			createFTContext();
		} else {
			ftContext = new NoFTContext();
		}
	}

	protected abstract void createFTContext();

	public String getName() {
		return name;
	}

	public int getInstanceID() {
		return instanceID;
	}

	public StreamingRuntimeContext createRuntimeContext(String taskName,
			Map<String, OperatorState<?>> states) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(taskName, env, getUserCodeClassLoader(), states);
	}

	public abstract void initializeInvoke();

	@Override
	public void invoke() throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoked with instance id {}", "TASK", getName(), getInstanceID());
		}

		initializeInvoke();

		collector.initializeOutputSerializers();

		try {
			invokeUserFunction();
		} catch (Exception e) {
			collector.flushOutputs();
			throw new RuntimeException(e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoke finished instance id {}", "TASK", getName(), getInstanceID());
		}

		collector.flushOutputs();
	}

	@Override
	public StreamConfig getConfiguration() {
		return configuration;
	}

	public void setConfiguration(StreamConfig configuration) {
		this.configuration = configuration;
	}

	@Override
	public AbstractStreamCollector<OUT, ?> getOutputCollector() {
		return collector;
	}

	public ClassLoader getUserClassLoader() {
		return userClassLoader;
	}

	@Override
	public AckerCollector getAckerCollector() {
		return ackerCollector;
	}

}
