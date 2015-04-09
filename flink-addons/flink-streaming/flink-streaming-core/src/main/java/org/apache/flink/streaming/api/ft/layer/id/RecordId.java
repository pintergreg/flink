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

package org.apache.flink.streaming.api.ft.layer.id;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

public class RecordId implements IOReadableWritable, Serializable, Comparable<RecordId> {

	private static final long serialVersionUID = 1L;

	private static Random random = new Random();

	private long currentRecordId;
	private long sourceRecordId;

	public RecordId() {
	}

	public RecordId(long currentRecordId, long sourceRecordId) {
		this.currentRecordId = currentRecordId;
		this.sourceRecordId = sourceRecordId;
	}

	public static RecordId newSourceXorMessage(long offset) {
		RecordId sourceXorMessage = new RecordId();
		sourceXorMessage.currentRecordId = offset;
		sourceXorMessage.sourceRecordId = random.nextLong();
		return sourceXorMessage;
	}

	public static RecordId newSourceRecordId() {
		RecordId sourceXorMessage = new RecordId();
		long rnd = random.nextLong();
		sourceXorMessage.currentRecordId = rnd;
		sourceXorMessage.sourceRecordId = rnd;
		return sourceXorMessage;
	}

	public static RecordId newRecordId(long sourceRecordId) {
		RecordId sourceXorMessage = new RecordId();
		sourceXorMessage.currentRecordId = random.nextLong();
		sourceXorMessage.sourceRecordId = sourceRecordId;
		return sourceXorMessage;
	}

	public void setRecordIdToSourceRecordId() {
		currentRecordId = sourceRecordId;
	}

	public long getCurrentRecordId() {
		return currentRecordId;
	}

	public long getSourceRecordId() {
		return sourceRecordId;
	}

	public static SerializationDelegate<RecordId> createSerializationDelegate() {
		TypeInformation<RecordId> typeInfo = TypeExtractor.getForObject(new RecordId());
		return new SerializationDelegate<RecordId>(typeInfo.createSerializer());
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(currentRecordId);
		out.writeLong(sourceRecordId);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		currentRecordId = in.readLong();
		sourceRecordId = in.readLong();
	}

	public RecordId copy() {
		return new RecordId(currentRecordId, sourceRecordId);
	}

	@Override
	public String toString() {
		return "s:" + Long.toHexString(sourceRecordId) + "\txor\t" + Long.toHexString(currentRecordId);
	}

	@Override
	public boolean equals(Object otherId) {
		if (otherId == null) {
			return false;
		} else if (this.getClass() == otherId.getClass()) {
			return this.currentRecordId == ((RecordId) otherId).getCurrentRecordId()
					&& this.sourceRecordId == ((RecordId) otherId).getSourceRecordId();
		}
		return false;
	}

	@Override
	public int compareTo(RecordId other) {
		if (sourceRecordId < other.sourceRecordId
				|| (sourceRecordId == other.sourceRecordId && currentRecordId < other.currentRecordId)) {
			return -1;
		} else if (this.equals(other)) {
			return 0;
		} else {
			return 1;
		}
	}

	@Override
	public int hashCode() {
		return (int) currentRecordId;
	}

	public static RecordId newRootId() {
		RecordId rid = new RecordId();
		long rnd = random.nextLong() & 0xFFFFFFFFFFFFFFFEL; //for setting LSB to zero
		rid.currentRecordId = rnd;
		rid.sourceRecordId = rnd;
		return rid;
	}

	public static RecordId newReplayedRootId(long originalRootId) {
		RecordId rid = new RecordId();
		long rrid = originalRootId | 0x1L; //for setting LSB to one
		rid.currentRecordId = rrid;
		rid.sourceRecordId = rrid;
		return rid;
	}

	public static RecordId newReplayableRecordId(long sourceRecordId, long parentRecordId, int nodeId, int chidRecordCounter) {
		RecordId rid = new RecordId();
		String str = String.valueOf(parentRecordId) + String.valueOf(nodeId) + String.valueOf(chidRecordCounter);

		//TODO chnage nasty debug
		System.out.println("  NEWID_COMPONENTS:" + String.valueOf(parentRecordId) + "," + String.valueOf(nodeId) + "," + String.valueOf(chidRecordCounter));

		rid.currentRecordId = redis.clients.util.MurmurHash.hash64A(str.getBytes(),0x5EED);

		System.out.println("    NEWID:" + rid.currentRecordId );
		rid.sourceRecordId = sourceRecordId;
		return rid;
	}

}
