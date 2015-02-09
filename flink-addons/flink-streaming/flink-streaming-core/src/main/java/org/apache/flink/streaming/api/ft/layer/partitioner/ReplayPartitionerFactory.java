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


package org.apache.flink.streaming.api.ft.layer.partitioner;

import org.apache.flink.streaming.partitioner.StreamPartitioner.PartitioningStrategy;

public class ReplayPartitionerFactory {

	public static ReplayPartitioner getReplayPartitioner(PartitioningStrategy partitioningStrategy) {
		ReplayPartitioner replayPartitioner;
		if (partitioningStrategy == null) {
			return new ReplayDistributePartitioner();
		}
		switch (partitioningStrategy) {
			case FORWARD:
				replayPartitioner = new ReplayDistributePartitioner();
				break;
			case DISTRIBUTE:
				replayPartitioner = new ReplayDistributePartitioner();
				break;
			case GLOBAL:
				replayPartitioner = new ReplayGlobalPartitioner();
				break;
			case BROADCAST:
				replayPartitioner = new ReplayBroadcastPartitioner();
				break;
			case SHUFFLE:
				replayPartitioner = new ReplayShufflePartitioner();
				break;
			case GROUPBY:
				replayPartitioner = new ReplayFieldsPartitioner();
				break;
			default:
				replayPartitioner = new ReplayDistributePartitioner();
				break;
		}
		return replayPartitioner;
	}
}
