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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.streaming.partitioner.StreamPartitioner.PartitioningStrategy;

public class OpFTLayerBuilder implements FTLayerBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(OpFTLayerBuilder.class);

	private StreamGraph streamGraph;

	protected AbstractJobVertex ftLayerVertex;
	private Map<String, AbstractJobVertex> streamVertices;
	protected Set<String> sourceVertices;

	private HashMap<String, Integer> ftLayerOutputs;

	public OpFTLayerBuilder(StreamingJobGraphGenerator jobGraphGenerator) {
		this.streamGraph = jobGraphGenerator.getStreamGraph();
		this.streamVertices = jobGraphGenerator.getStreamVertices();
		this.sourceVertices = jobGraphGenerator.getSourceVertices();
		this.ftLayerOutputs = new HashMap<String, Integer>();
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

		//T->P
		ArrayList<ArrayList<Integer>> sourceSuccessives = new ArrayList<ArrayList<Integer>>();
		Map<Integer, PartitioningStrategy> partitioningStrategies = new HashMap<Integer, PartitioningStrategy>();
		Set<String> processingTaskVertices = ftLayerOutputs.keySet();
//		for (String upStreamVertexName : sourceVertices) {
//			List<String> outputs = streamGraph.getOutEdges(upStreamVertexName);
//			ArrayList<Integer> list = new ArrayList<Integer>();
//
//			sourceSuccessives.add(list);
//
//			for (String downStreamVertexName : outputs) {
//				if (processingTaskVertices.contains(downStreamVertexName)) {
//					list.add(ftLayerOutputs.get(downStreamVertexName));
//
//					partitioningStrategies.put(ftLayerOutputs.get(downStreamVertexName), streamGraph.getOutPartitioner(upStreamVertexName, downStreamVertexName)
//							.getStrategy());
//				}
//			}
//
//		}

		//Map<Integer, PartitioningStrategy> helyett legyen Map<Integer, Map<Integer, PartitioningStrategy>>
		//(S->T)->P kéne, de ehelyett ez nem S->(T->P)??? 2. jó
		Map<Integer, Map<Integer, PartitioningStrategy>> pStrategies = new HashMap<Integer, Map<Integer, PartitioningStrategy>>();

		for (String upStreamVertexName : sourceVertices){
			List<String> outputs = streamGraph.getOutEdges(upStreamVertexName);
			ArrayList<Integer> list = new ArrayList<Integer>();

			sourceSuccessives.add(list);

			List<String> vertexNames = new ArrayList<String>();
			vertexNames.add(upStreamVertexName);
			Map<Integer, PartitioningStrategy> strategiesOfTasks = new HashMap<Integer, PartitioningStrategy>();

			for(String downStreamVertexName : outputs) {
				if (processingTaskVertices.contains(downStreamVertexName)) {
					list.add(ftLayerOutputs.get(downStreamVertexName));

					strategiesOfTasks.put(ftLayerOutputs.get(downStreamVertexName), streamGraph.getOutPartitioner(upStreamVertexName, downStreamVertexName)
							.getStrategy());
				}
			}

			//S (upStreamVertexName) sorszáma kell még, de a (T->P) már megvan (strategiesOfTask)
			pStrategies.put(vertexNames.indexOf(upStreamVertexName), strategiesOfTasks);
		}


//		for (int i = 0; i < sourceVertices.size(); i++) {
//			Map<Integer, PartitioningStrategy> strategiesOfTasks = new HashMap<Integer, PartitioningStrategy>();
//			for (int j = 0; j < sourceSuccessives.get(i).size(); j++) {
//				strategiesOfTasks.put(j, partitioningStrategies.get(j));//átszámozom, másik lenne, hogy foreach-ezek, akkor az eredeti számozás maradna
//			}
//			pStrategies.put(i, strategiesOfTasks);
//		}


		FTLayerConfig ftLayerConfig = new FTLayerConfig(ftLayerVertex.getConfiguration());
		ftLayerConfig.setNumberOfOutputs(processingTaskVertices.size());
		ftLayerConfig.setSourceSuccessives(sourceSuccessives);
//		ftLayerConfig.setPartitioningStrategies(partitioningStrategies);
		ftLayerConfig.setPartitioningStrategies(pStrategies);
	}

	@Override
	public FTStatus getStatus() {
		return FTStatus.ON;
	}

	@SuppressWarnings("unused")
	public AbstractJobVertex getFtLayerVertex() {
		return ftLayerVertex;
	}
}
