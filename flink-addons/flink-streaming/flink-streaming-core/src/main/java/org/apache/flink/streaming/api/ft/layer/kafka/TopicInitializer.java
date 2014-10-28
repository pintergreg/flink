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

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class TopicInitializer {

	private static class ZkSer implements ZkSerializer {
		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			return ZKStringSerializer.serialize(data);
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			return ZKStringSerializer.deserialize(bytes);
		}
	}

	public static void createTopic(String zk, String topic, int numberOfPartitions,
			int replicationFactor) {
		int sessionTimeoutMs = 10000;
		int connectionTimeoutMs = 10000;

		ZkClient zkClient = new ZkClient(zk, sessionTimeoutMs, connectionTimeoutMs,
				new ZkSer());

		AdminUtils.createTopic(zkClient, topic, numberOfPartitions, replicationFactor,
				new Properties());
	}
	
}
