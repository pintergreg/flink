/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer.event;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;

public class XorEvent extends TaskEvent {

	private RecordId recordId;
	private boolean failFlag = false;

	public XorEvent() {
		super();
		setRecordId(new RecordId());
	}

	public XorEvent(RecordId recordId, boolean failFlag) {
		super();
		this.recordId = recordId;
		this.failFlag = failFlag;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		recordId.write(out);
		out.writeBoolean(failFlag);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		recordId.read(in);
		failFlag = in.readBoolean();
	}

	public RecordId getRecordId() {
		return recordId;
	}

	public void setRecordId(RecordId recordId) {
		this.recordId = recordId;
	}

	public boolean getFailFlag() {
		return this.failFlag;
	}

}
