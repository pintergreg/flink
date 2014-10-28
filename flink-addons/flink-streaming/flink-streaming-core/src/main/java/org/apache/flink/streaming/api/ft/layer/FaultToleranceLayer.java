package org.apache.flink.streaming.api.ft.layer;

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

import java.util.ArrayList;
import java.util.HashMap;

public class FaultToleranceLayer implements AbstractFaultToleranceLayer {

	private static final long serialVersionUID = 1L;

	HashMap<Long, ArrayList<byte[]>> topics;

	public FaultToleranceLayer() {
		this.topics = new HashMap<Long, ArrayList<byte[]>>();
	}

	@Override
	public void createNewTopic(long topicId) {
		topics.put(topicId, new ArrayList<byte[]>());
	}

	@Override
	public void consume(long topicId, byte[] out) {
		topics.get(topicId).add(out);

	}

	@Override
	public <T> FaultToleranceLayerIterator<T> iterator(long topicId) {
		if (topics.containsKey(topicId)) {
			return new FaultToleranceLayerIterator<T>(topics.get(topicId));
		} else {
			throw new RuntimeException("Topic '" + topicId + "' does not exist!");
		}
	}

	@Override
	public void remove(long topicId) {
		topics.remove(topicId);
	}

}
