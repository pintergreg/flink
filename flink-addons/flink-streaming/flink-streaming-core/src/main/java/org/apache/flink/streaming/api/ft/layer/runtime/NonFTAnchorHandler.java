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

package org.apache.flink.streaming.api.ft.layer.runtime;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.streamrecord.IdentifiableStreamRecord;

public class NonFTAnchorHandler implements AnchorHandler {

	@Override
	public void setAnchorRecord(IdentifiableStreamRecord anchorRecord) {
	}

	@Override
	public RecordId getAnchorRecord() {
		return null;
	}

	/*
	 * Additional parameters (instanceID, childRecordCounter) are added for deterministic ID generation
	 */
	@Override
	public RecordId setOutRecordId(SerializationDelegate<? extends IdentifiableStreamRecord> outRecord, int instanceID, int childRecordCounter, boolean isItSource) {
		return null;
	}

}