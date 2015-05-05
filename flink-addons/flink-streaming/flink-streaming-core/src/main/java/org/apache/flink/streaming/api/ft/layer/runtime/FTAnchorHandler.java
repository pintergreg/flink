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

public class FTAnchorHandler implements AnchorHandler {
	protected RecordId anchorRecordId;

	@Override
	public void setAnchorRecord(IdentifiableStreamRecord anchorRecord) {
		if (anchorRecord != null) {
			this.anchorRecordId = anchorRecord.getId();
		}
	}

	@Override
	public RecordId getAnchorRecord() {
		return anchorRecordId;
	}

	@Override
	public RecordId setOutRecordId(SerializationDelegate<? extends IdentifiableStreamRecord> outRecord, int instanceID, int childRecordCounter, boolean isItSource) {
		if (anchorRecordId != null) {

			long sourceRecordId = anchorRecordId.getSourceRecordId();
		/* Give parameters for deterministic ID generation.
		 *  0. root ID
		 *  1. parent record ID
		 *  2. instance ID
		 *  3. child record counter, that is an ordering of egress records
		 *  4. node type (true is source)
		 *  Instance ID and the child record counter comes from StreamOutput, parent record ID can be passed here
		 */
			return outRecord.getInstance().newId(sourceRecordId, anchorRecordId.getCurrentRecordId(), instanceID, childRecordCounter, isItSource);
		}else{
			//WTF
			//System.err.println("WTF");
			return new RecordId().newRootId();
		}
	}

}