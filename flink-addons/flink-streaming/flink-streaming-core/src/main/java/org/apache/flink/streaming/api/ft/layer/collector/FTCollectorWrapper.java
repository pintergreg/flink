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

package org.apache.flink.streaming.api.ft.layer.collector;

import org.apache.flink.streaming.api.ft.layer.AbstractFT;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class FTCollectorWrapper<T> implements Collector<T> {

	private Collector<T> outerCollector;
	private AbstractFT<T> abstractFT;
	private StreamRecord<T> streamRecord;

	public FTCollectorWrapper(Collector<T> outerCollector, AbstractFT<T> abstractFT) {
		this.streamRecord = new StreamRecord<T>();
		this.outerCollector = outerCollector;
		this.abstractFT = abstractFT;
	}

	@Override
	public void collect(T record) {

		streamRecord.setObject(record);
		abstractFT.persist(streamRecord);
		outerCollector.collect(record);
	}

	@Override
	public void close() {
		outerCollector.close();
	}
}