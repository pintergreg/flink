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

import java.util.HashMap;
import java.util.Random;

import org.apache.flink.streaming.api.ft.layer.util.PersistentStorage;
import org.apache.flink.streaming.api.ft.layer.util.ResetId;
import org.apache.flink.streaming.api.ft.layer.util.SerializedRecordWithId;

public class FaultToleranceLayer implements AbstractFaultToleranceLayer {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, PersistentStorage> sourceIdToRecords;
	private HashMap<Long, ResetId> sourceRecordIdToResetId;
	private HashMap<Integer, AbstractFaultToleranceLayerIterator<?>> sourceIdToIterator;

	private Random random;

	public FaultToleranceLayer() {
		this.sourceIdToRecords = new HashMap<Integer, PersistentStorage>();
		this.sourceRecordIdToResetId = new HashMap<Long, ResetId>();
		this.sourceIdToIterator = new HashMap<Integer, AbstractFaultToleranceLayerIterator<?>>();

		this.random = new Random();
	}

	@Override
	public void createNewSource(int sourceId) {
		sourceIdToRecords.put(sourceId, new PersistentStorage());
	}

	@Override
	public void push(int sourceId, byte[] out) {
		long recordId = random.nextLong();
		PersistentStorage source = sourceIdToRecords.get(sourceId);

		source.push(new SerializedRecordWithId(out, recordId));
		long offset = source.lastOffset();

		sourceRecordIdToResetId.put(recordId, new ResetId(sourceId, offset));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> FaultToleranceLayerIterator<T> iterator(int sourceId) {
		PersistentStorage source = sourceIdToRecords.get(sourceId);

		if (source != null) {
			FaultToleranceLayerIterator<T> iterator = (FaultToleranceLayerIterator<T>) sourceIdToIterator
					.get(sourceId);

			if (iterator == null) {
				iterator = new FaultToleranceLayerIterator<T>(source);
				sourceIdToIterator.put(sourceId, iterator);
			}

			return iterator;
		} else {
			throw new RuntimeException("Topic '" + sourceId + "' does not exist!");
		}
	}

	public void reset(long sourceRecordId) {
		ResetId resetId = sourceRecordIdToResetId.get(sourceRecordId);
		AbstractFaultToleranceLayerIterator<?> iterator = sourceIdToIterator.get(resetId
				.getSourceId());
		iterator.reset(resetId.getOffset());
	}

	public void ack(long sourceRecordId) {
		ResetId resetId = sourceRecordIdToResetId.get(sourceRecordId);
		sourceIdToRecords.get(resetId.getSourceId()).remove(resetId.getOffset());
	}

	@Override
	public void removeSource(int sourceId) {
		sourceIdToRecords.remove(sourceId);
	}

}
