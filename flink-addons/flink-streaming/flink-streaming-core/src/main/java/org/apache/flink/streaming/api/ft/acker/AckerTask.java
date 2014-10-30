/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.acker;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.streaming.api.ft.layer.FaultToleranceLayer;
import org.apache.flink.streaming.api.ft.layer.util.SourceRecordMessage;
import org.apache.flink.streaming.api.ft.layer.util.XorMessage;

public class AckerTask implements Serializable {

	private static final long serialVersionUID = 1L;

	private FaultToleranceLayer ftLayer;
	HashMap<Long, Long> ackerTable;

	public AckerTask(FaultToleranceLayer ftLayer) {
		this.ftLayer = ftLayer;
		this.ackerTable = new HashMap<Long, Long>();
	}

	public void newSourceRecord(SourceRecordMessage sourceRecordMessage) {
		long sourceRecordId = sourceRecordMessage.getSourceRecordId();
		ackerTable.put(sourceRecordId, sourceRecordId);
	}

	public void xor(XorMessage xorMessage) {
		long sourceRecordId = xorMessage.getSourceRecordId();
		long newAckValue = ackerTable.get(sourceRecordId) ^ xorMessage.getRecordId();
		if (newAckValue == 0L) {
			ack(sourceRecordId);
			ackerTable.remove(sourceRecordId);
		} else {
			// TODO if timeout then fail
			ackerTable.put(sourceRecordId, newAckValue);
		}
	}

	public void fail(long sourceRecordId) {
		ftLayer.reset(sourceRecordId);
	}

	public void ack(long sourceRecordId) {
		ftLayer.ack(sourceRecordId);
	}

	public long getAckValue(long sourceRecordId) {
		return ackerTable.get(sourceRecordId);
	}
}
