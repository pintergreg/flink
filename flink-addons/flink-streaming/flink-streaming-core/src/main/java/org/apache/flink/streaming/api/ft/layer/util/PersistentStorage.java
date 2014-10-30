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

import java.util.LinkedHashMap;
import java.util.PriorityQueue;

public class PersistentStorage {
	
	private LinkedHashMap<Long, SerializedRecordWithId> backup;
	private PriorityQueue<Long> canBeRemoved;
	private long eldestOffset;
	private long nextOffset;
	
	public PersistentStorage() {
		this.backup = new LinkedHashMap<Long, SerializedRecordWithId>();
		this.canBeRemoved = new PriorityQueue<Long>();
		this.nextOffset = 0L;
		this.eldestOffset = 0L;
	}

	public void push(SerializedRecordWithId serializedRecordWithId) {
		backup.put(nextOffset, serializedRecordWithId);
		nextOffset++;
	}

	public long eldestOffset() {
		return eldestOffset;
	}

	public long lastOffset() {
		return nextOffset - 1;
	}

	public int size() {
		return backup.size();
	}

	public SerializedRecordWithId get(long i) {
		return backup.get(i);
	}

	public void remove(long i) {
		canBeRemoved.add(i);

		while (!canBeRemoved.isEmpty() && canBeRemoved.peek() == eldestOffset) {
			canBeRemoved.poll();
			backup.remove(eldestOffset);
			eldestOffset++;
		}
	}
}
