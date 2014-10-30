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
import org.apache.flink.streaming.api.ft.layer.util.PersistentStorage;
import org.apache.flink.streaming.api.ft.layer.util.RecordWithId;
import org.apache.flink.streaming.api.ft.layer.util.SerializedRecordWithId;

public class FaultToleranceLayerIterator<T> extends AbstractFaultToleranceLayerIterator<T> {

	private static final long serialVersionUID = 1L;

	private PersistentStorage list;
	long offset;

	FaultToleranceLayerIterator(PersistentStorage list) {
		this.list = list;
		this.offset = 0;
	}

	@Override
	public boolean hasNext() {
		return offset <= list.lastOffset();
	}

	@Override
	public T next() {
		return deserialize(list.get(offset++).getRecord());
	}

	@Override
	public RecordWithId<T> nextWithId() {
		SerializedRecordWithId serializedRecord = list.get(offset++);
		return new RecordWithId<T>(deserialize(serializedRecord.getRecord()), serializedRecord.getId());
	}

	@SuppressWarnings("unchecked")
	private T deserialize(byte[] bytes) {
		return (T) SerializationUtils.deserialize(bytes);
	}
	
	@Override
	public void reset(long offset) {
		if (offset < 0 || offset > list.size()) {
			throw new RuntimeException("Offset is out of bound!");
		} else {
			this.offset = offset;
		}
	}

	@Override
	public void initializeFromBeginning() {
		offset = list.eldestOffset();
	}

	@Override
	public void initializeFromCurrent() {
		offset = list.size();
	}
	
	@Override
	public void initializeFromOffset(long offset) {
		reset(offset);
	}


	@Override
	public long getLastOffset() {
		return list.size() - 1;
	}

	@Override
	public long currentOffset() {
		return offset;
	}
}
