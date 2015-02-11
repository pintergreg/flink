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

package org.apache.flink.streaming.api.ft.layer.event;

import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XorEventListener implements EventListener {
	private final static Logger LOG = LoggerFactory.getLogger(XorEventListener.class);

	private FTLayer ftLayer;

	public XorEventListener(FTLayer ftLayer) {
		this.ftLayer = ftLayer;
	}

	@Override
	public void onEvent(Object event) {
		XorEvent xorEvent = (XorEvent) event;
		RecordId recordId = xorEvent.getRecordId();
		boolean failFlag = xorEvent.getFailFlag();
		if (failFlag == true) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("FAIL-event: {}", xorEvent.getRecordId());
			}
			ftLayer.explicitFail(recordId.getSourceRecordId());
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("XOR-event: {}", xorEvent.getRecordId());
			}
			ftLayer.xor(recordId);
		}
	}
}
