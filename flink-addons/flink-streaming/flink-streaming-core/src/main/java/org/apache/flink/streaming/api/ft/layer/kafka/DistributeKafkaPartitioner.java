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

package org.apache.flink.streaming.api.ft.layer.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class DistributeKafkaPartitioner implements Partitioner {
	private int currentPartition;
	
	public DistributeKafkaPartitioner(VerifiableProperties props) {
		currentPartition = 0;
	}

	@Override
	public int partition(Object key, int a_numPartitions) {
		int partition = currentPartition;
		
		currentPartition++;
		if (currentPartition == a_numPartitions) {
			currentPartition = 0;
		}

		return partition;
	}
}