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

package org.apache.flink.streaming.api.ft.layer.util;

import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.streaming.api.ft.layer.Xorer;
import org.apache.flink.streaming.api.ft.layer.event.XorEvent;

public class TaskFTXorer extends Xorer {

	private MutableRecordReader<?> recordReader;

	public TaskFTXorer(MutableRecordReader<?> bufferReader) {
		super();
		this.recordReader = bufferReader;
	}

	@Override
	protected void emit(RecordId recordId) throws Exception {
		recordReader.sendTaskEvent(new XorEvent(recordId, false));
	}

	@Override
	protected void fail(RecordId recordId) throws Exception {
		recordReader.sendTaskEvent(new XorEvent(recordId, true));
	}

}
