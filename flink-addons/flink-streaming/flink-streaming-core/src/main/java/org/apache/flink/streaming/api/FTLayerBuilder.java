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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.partitioner.StreamPartitioner;

import java.io.Serializable;

public interface FTLayerBuilder {

	public enum FTStatus implements Serializable {
		ON, OFF
	}

	public boolean isChainingEnabled(String vertexName, String outName);

	void createFTLayerVertex(JobGraph jobGraph, int parallelism);

	void connectWithFTLayer(String vertexName);

	public void setSourceSuccessives();

	public FTStatus getStatus();

	public void addEdgeInformation(String sourceName, String taskName, StreamPartitioner.PartitioningStrategy partitionStrategy);
}
