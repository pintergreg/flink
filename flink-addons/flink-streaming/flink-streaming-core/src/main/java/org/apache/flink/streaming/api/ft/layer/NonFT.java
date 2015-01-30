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

import org.apache.flink.streaming.api.ft.layer.collector.NonFTCollectorWrapper;
import org.apache.flink.streaming.api.ft.layer.util.NonFTAnchorer;
import org.apache.flink.streaming.api.ft.layer.util.NonFTPersister;
import org.apache.flink.streaming.api.ft.layer.util.NonFTXorer;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.util.Collector;

public class NonFT<T> extends AbstractFT<T> {

	public NonFT() {
		super(new NonFTPersister<T>(), new NonFTXorer(), new NonFTAnchorer());
	}

	@Override
	protected void emit(RecordId recordId) throws Exception {
	}

	@Override
	protected void fail(RecordId recordId) throws Exception {
	}

	@Override
	public Collector<T> wrap(Collector<T> collector) {
		return new NonFTCollectorWrapper(collector);
	}

	@Override
	public boolean faultToleranceIsOn() {
		return false;
	}

	@Override
	public void fail() {
	}

}

