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
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.ft.layer.FaultToleranceLayerCollector;

public class KafkaLayerCollector<OUT> implements FaultToleranceLayerCollector<OUT> {

	private static final long serialVersionUID = 1L;

	private transient Producer<Integer, byte[]> producer;
	private Properties props;
	private String topicId;

	public KafkaLayerCollector(String topicId, String brokerAddr) {
		this.topicId = topicId;
		props = new Properties();
		props.put("metadata.broker.list", brokerAddr);
		props.put("request.required.acks", "1");
		initialize();
	}

	public KafkaLayerCollector(String topicId, KafkaProperties props) {
		this.props = props.getProperties();
		this.topicId = topicId;
		initialize();
	}

	public KafkaLayerCollector(String topicId, String... props) {
		this.props = createProperties(props);
		this.topicId = topicId;
		initialize();
	}

	public void initialize() {
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<Integer, byte[]>(config);
	}

	@Override
	public void collect(OUT record) {
		byte[] out = SerializationUtils.serialize((Serializable) record);
		KeyedMessage<Integer, byte[]> data = new KeyedMessage<Integer, byte[]>(topicId, out);

		producer.send(data);
	}

	@Override
	public void close() {
		producer.close();
	}

	protected Properties createProperties(String... props) {
		Properties properties = new Properties();
		for (int i = 0; i < props.length; i++) {
			String[] pair = props[i].split("=");
			if (pair.length == 2) {
				properties.put(pair[0], pair[1]);
			}
		}
		return properties;
	}

}
