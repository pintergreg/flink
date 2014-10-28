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

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.ft.layer.MessageWithOffset;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceFromKafkaInvokable<OUT> extends StreamInvokable<OUT, OUT> implements
		Serializable {
	private static final long serialVersionUID = 1L;

	private String topicId;
	private final String host;
	private final int port;
	private KafkaLayerIterator iterator;

	public SourceFromKafkaInvokable(String topicId,
			String host, int port) {
		super(null);
		this.topicId = topicId;
		this.host = host;
		this.port = port;
	}

	private void initializeConnection() {
		iterator = new KafkaLayerIterator(host, port, topicId, 0, 100L);
		iterator.initializeFromCurrent();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void invoke() throws Exception {
		initializeConnection();
		
		while (iterator.hasNext()) {
			MessageWithOffset msg = iterator.nextWithOffset();
			OUT out = (OUT) SerializationUtils.deserialize(msg.getMessage());
			collector.collect(out);
		}
	}

	@Override
	protected void immutableInvoke() throws Exception {
	}

	@Override
	protected void mutableInvoke() throws Exception {
	}

	@Override
	protected void callUserFunction() throws Exception {
	}

}
