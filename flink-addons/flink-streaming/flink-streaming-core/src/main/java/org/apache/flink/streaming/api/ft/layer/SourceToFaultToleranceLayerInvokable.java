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

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceToFaultToleranceLayerInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;

	private SourceFunction<OUT> sourceFunction;
	private FaultToleranceLayerCollector<OUT> collector;

	public SourceToFaultToleranceLayerInvokable(SourceFunction<OUT> sourceFunction, FaultToleranceLayerCollector<OUT> collector) {
		super(sourceFunction);
		this.sourceFunction = sourceFunction;
		this.collector = collector;
	}

	@Override
	public void invoke() throws Exception {
		collector.initialize();
		sourceFunction.invoke(collector);
	}

	@Override
	protected void immutableInvoke() throws Exception {
		// intentionally empty
	}

	@Override
	protected void mutableInvoke() throws Exception {
		// intentionally empty
	}

	@Override
	protected void callUserFunction() throws Exception {
		// intentionally empty
	}

}