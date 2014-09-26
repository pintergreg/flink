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

package org.apache.flink.streaming.api.environment;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.util.ClusterUtil;

public class LambdaEnvironment {

	ExecutionEnvironment batchEnvironment;
	StreamExecutionEnvironment streamEnvironment;

	JobGraph lambdaGraph;

	public LambdaEnvironment(ExecutionEnvironment batchEnvironment,
			StreamExecutionEnvironment streamEnvironment) {
		this.batchEnvironment = batchEnvironment;
		this.streamEnvironment = streamEnvironment;
	}

	public void execute(String jobName) throws Exception {

		Plan plan = batchEnvironment.createProgramPlan(jobName);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		JobGraph batchGraph = jgg.compileJobGraph(op);

		JobGraph streamGraph = streamEnvironment.jobGraphBuilder.createJobGraph(jobName);

		// ClusterUtil.runOnMiniCluster(batchGraph,
		// streamEnvironment.getExecutionParallelism());
		// ClusterUtil.runOnMiniCluster(streamGraph,
		// streamEnvironment.getExecutionParallelism());

		lambdaGraph = createLambdaGraph(batchGraph, streamGraph);

		ClusterUtil.runOnMiniCluster(lambdaGraph, 2);
	}

	private JobGraph createLambdaGraph(JobGraph batchGraph, JobGraph streamGraph) {

		Map<String, AbstractJobVertex> streamVertices = new HashMap<String, AbstractJobVertex>();

		JobGraph lambdaGraph = new JobGraph(batchGraph.getName());

		for (AbstractJobVertex vertex : streamGraph.getVertices()) {
			lambdaGraph.addVertex(vertex);
			streamVertices.put(vertex.getName(), vertex);
		}

		for (AbstractJobVertex vertex : batchGraph.getVertices()) {
			lambdaGraph.addVertex(vertex);
			TaskConfig config = new TaskConfig(vertex.getConfiguration());
			String lambdaId = config.getLambdaID();

			if (streamVertices.containsKey(lambdaId)) {
				AbstractJobVertex streamVertex = streamVertices.get(lambdaId);
				StreamConfig sConfig = new StreamConfig(streamVertex.getConfiguration());
				streamVertex.connectNewDataSetAsInput(vertex, DistributionPattern.POINTWISE);
				
				sConfig.addLambdaInput();
				config.addLambdaOutput();
			}
		}

		return lambdaGraph;
	}
}
