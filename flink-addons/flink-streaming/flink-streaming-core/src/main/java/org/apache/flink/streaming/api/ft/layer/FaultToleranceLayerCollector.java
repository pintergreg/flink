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

package org.apache.flink.streaming.api.ft.layer;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;

public class FaultToleranceLayerCollector<OUT> implements AbstractFaultToleranceLayerCollector<OUT> {

	private static final long serialVersionUID = 1L;

	private AbstractFaultToleranceLayer ftLayer;
	private int sourceId;

	public FaultToleranceLayerCollector(AbstractFaultToleranceLayer ftLayer, int sourceId) {
		this.ftLayer = ftLayer;
		this.sourceId = sourceId;
		initialize();
	}

	public void initialize() {
		ftLayer.createNewSource(sourceId);
	}

	@Override
	public void collect(OUT record) {
		byte[] out = serialize(record);
		ftLayer.push(sourceId, out);
	}

	@Override
	public void close() {

	}

	public void remove() {
		ftLayer.removeSource(sourceId);
	}

	@Override
	public byte[] serialize(OUT record) {
		return SerializationUtils.serialize((Serializable) record);
	}
}
