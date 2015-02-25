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

package org.apache.flink.streaming.api.ft.layer.util;

import org.apache.flink.streaming.partitioner.StreamPartitioner.PartitioningStrategy;

public class FTEdgeInformation {

	private int sourceID;
	private int taskID;
	private PartitioningStrategy partitioningStrategy;

	public FTEdgeInformation() {

	}

	public FTEdgeInformation(int sourceID, int taskID, PartitioningStrategy partitioningStrategy) {
		this.sourceID = sourceID;
		this.taskID = taskID;
		this.partitioningStrategy = partitioningStrategy;
	}

	public int getSourceID() {
		return sourceID;
	}

	public void setSourceID(int sourceID) {
		this.sourceID = sourceID;
	}

	public int getTaskID() {
		return taskID;
	}

	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}

	public PartitioningStrategy getPartitioningStrategy() {
		return partitioningStrategy;
	}

	public void setPartitioningStrategy(PartitioningStrategy partitioningStrategy) {
		this.partitioningStrategy = partitioningStrategy;
	}
}
