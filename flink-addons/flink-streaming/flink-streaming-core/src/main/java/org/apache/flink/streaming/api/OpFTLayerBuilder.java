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

package org.apache.flink.streaming.api;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerConfig;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerVertex;
import org.apache.flink.streaming.api.ft.layer.util.FTEdgeInformation;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OpFTLayerBuilder implements FTLayerBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(OpFTLayerBuilder.class);

	private StreamGraph streamGraph;

	protected AbstractJobVertex ftLayerVertex;
	private Map<String, AbstractJobVertex> streamVertices;
	protected Set<String> sourceVertices;

	//used for indexing inputs and outputs
	private HashMap<String, Integer> ftLayerOutputs;
	private HashMap<String, Integer> ftLayerInputs;

	//stores edge information
	private List<FTEdgeInformation> ftLayerEdgeInformations;

	public OpFTLayerBuilder(StreamingJobGraphGenerator jobGraphGenerator) {
		this.streamGraph = jobGraphGenerator.getStreamGraph();
		this.streamVertices = jobGraphGenerator.getStreamVertices();
		this.sourceVertices = jobGraphGenerator.getSourceVertices();
		this.ftLayerOutputs = new HashMap<String, Integer>();

		this.ftLayerInputs = new HashMap<String, Integer>();
		this.ftLayerEdgeInformations = new ArrayList<FTEdgeInformation>();
	}

	@Override
	public boolean isChainingEnabled(String vertexName, String outName) {
		return !sourceVertices.contains(vertexName);
	}

	@Override
	public void createFTLayerVertex(JobGraph jobGraph, int parallelism) {
		String vertexName = "FTLayerVertex";
		Class<? extends AbstractInvokable> vertexClass = FTLayerVertex.class;
		ftLayerVertex = new AbstractJobVertex(vertexName);
		jobGraph.addVertex(ftLayerVertex);
		ftLayerVertex.setInvokableClass(vertexClass);
		ftLayerVertex.setParallelism(parallelism);
		if (LOG.isDebugEnabled()) {
			LOG.debug("FTLayer parallelism set: {} for {}", parallelism, vertexName);
		}
		FTLayerConfig config = new FTLayerConfig(ftLayerVertex.getConfiguration());
		config.setNumberOfSources(sourceVertices.size());
		config.setBufferTimeout(100L);
	}

	@Override
	public void connectWithFTLayer(String vertexName) {
		if (sourceVertices.contains(vertexName)) {
			setFTLayerInput(vertexName);
			ftLayerInputs.put(vertexName, ftLayerInputs.size());
		} else {
			setFTLayerOutput(vertexName);
			ftLayerOutputs.put(vertexName, ftLayerOutputs.size());
		}
	}

	private void setFTLayerOutput(String vertexName) {
		AbstractJobVertex upStreamVertex = ftLayerVertex;
		AbstractJobVertex downStreamVertex = streamVertices.get(vertexName);
		downStreamVertex.connectNewDataSetAsInput(upStreamVertex, DistributionPattern.ALL_TO_ALL);

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED FTLayer to: {}", vertexName);
		}
	}

	private void setFTLayerInput(String vertexName) {
		AbstractJobVertex upStreamVertex = streamVertices.get(vertexName);
		AbstractJobVertex downStreamVertex = ftLayerVertex;

		downStreamVertex.connectNewDataSetAsInput(upStreamVertex, DistributionPattern.ALL_TO_ALL);
		StreamConfig upStreamConfig = new StreamConfig(upStreamVertex.getConfiguration());
		KeySelector<?, ?> keySelector = streamGraph.getKeySelector(vertexName);
		upStreamConfig.setKeySelector(keySelector);
		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED to FTLayer: {}", vertexName);
		}
	}

	@Override
	public void setSourceSuccessives() {
		//TODO ezt a metódust átnevezni (vagy megszűntetni)
		Set<String> processingTaskVertices = ftLayerOutputs.keySet();

		FTLayerConfig ftLayerConfig = new FTLayerConfig(ftLayerVertex.getConfiguration());
		ftLayerConfig.setNumberOfOutputs(processingTaskVertices.size());

		//stores edge information in the config
		ftLayerConfig.setEdgeInformations(ftLayerEdgeInformations);
	}

	@Override
	public FTStatus getStatus() {
		return FTStatus.ON;
	}

	/**
	 * Adds information (sourceID, taskID and a {@link StreamPartitioner.PartitioningStrategy})
	 * only about "Source to Task" edges. This information is used to set the RecordWriters with the
	 * correct Partition Strategy from the Fault Tolerance Layer to the source successive tasks.
	 * Gets three parameters:
	 *
	 * @param sourceName
	 * 		- mapped to an lower level index, and that is stored
	 * @param taskName
	 * 		- mapped to an lower level index, and that is stored
	 * @param partitionStrategy
	 * 		- Partition Strategy between the source and the successive task.
	 */
	@Override
	public void addEdgeInformation(String sourceName, String taskName, StreamPartitioner.PartitioningStrategy partitionStrategy) {
		System.out.println(sourceName + "->" + taskName + ", ps:" + partitionStrategy.name());
		if (ftLayerInputs.containsKey(sourceName) && ftLayerOutputs.containsKey(taskName)) {
			System.out.println("EDGE_FROM_SOURCE::" + sourceName + "->" + taskName);
			ftLayerEdgeInformations.add(new FTEdgeInformation(
							ftLayerInputs.get(sourceName),
							ftLayerOutputs.get(taskName),
							streamGraph.getOutPartitioner(sourceName, taskName).getStrategy())
			);
		}
	}

	@SuppressWarnings("unused")
	public AbstractJobVertex getFtLayerVertex() {
		return ftLayerVertex;
	}
}
