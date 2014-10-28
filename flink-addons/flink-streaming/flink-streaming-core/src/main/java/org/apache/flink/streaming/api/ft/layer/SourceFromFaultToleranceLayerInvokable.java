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

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceFromFaultToleranceLayerInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;

	private FaultToleranceLayerIterator iterator;

	public SourceFromFaultToleranceLayerInvokable(FaultToleranceLayerIterator iterator) {
		super(null);
		this.iterator = iterator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void invoke() throws Exception {
		iterator.initializeFromCurrent();

		while (iterator.hasNext()) {
			MessageWithOffset msg = iterator.nextWithOffset();
			OUT out = (OUT) SerializationUtils.deserialize(msg.getMessage());
			collector.collect(out);
		}
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
