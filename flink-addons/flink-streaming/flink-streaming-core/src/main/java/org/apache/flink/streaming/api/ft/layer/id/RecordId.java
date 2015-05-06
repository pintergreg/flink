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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

public class RecordId implements IOReadableWritable, Serializable, Comparable<RecordId> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RecordId.class);

	private static Random random = new Random();

	private long currentRecordId;
	private long sourceRecordId;

	public RecordId() {
	}

	public RecordId(long currentRecordId, long sourceRecordId) {
		this.currentRecordId = currentRecordId;
		this.sourceRecordId = sourceRecordId;
	}

	/*
	 * the old ID generation method, not used in production code anymore
	 */
	public static RecordId newSourceXorMessage(long offset) {
		RecordId sourceXorMessage = new RecordId();
		sourceXorMessage.currentRecordId = offset;
		sourceXorMessage.sourceRecordId = random.nextLong();
		return sourceXorMessage;
	}

	/*
	 * the old ID generation method, not used in production code anymore
	 */
	public static RecordId newSourceRecordId() {
		RecordId sourceXorMessage = new RecordId();
		long rnd = random.nextLong();
		sourceXorMessage.currentRecordId = rnd;
		sourceXorMessage.sourceRecordId = rnd;
		return sourceXorMessage;
	}

	/*
	 * the old ID generation method, not used in production code anymore
	 */
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

	/**
	 * Generates new ID for a root record
	 *
	 * @return a RecordId, with a new random selected 64 bit value
	 */
	public static RecordId newRootId() {
		RecordId rid = new RecordId();
		long rnd = random.nextLong() & 0xFFFFFFFFFFFFFFFEL; //for setting LSB to zero
		rid.currentRecordId = rnd;
		rid.sourceRecordId = rnd;
		return rid;
	}

	/**
	 * Copies the original record ID to the "new", replayed record.
	 *
	 * @param originalRootId
	 * 		- the 64 bit ID of the original record, that will be replayed
	 * @return a RecordId
	 */
	public static RecordId newReplayedRootId(long originalRootId) {
		RecordId rid = new RecordId();
		rid.currentRecordId = originalRootId;
		rid.sourceRecordId = originalRootId;//  | 0x1L; //for setting LSB to one
		//System.err.println(originalRootId + "," +(originalRootId  | 0x1L));
		return rid;
	}

	/**
	 * Generated a new record ID for a non-root records in a deterministic way with a 64 bit hash
	 * function from some paramteres:
	 *
	 * @param sourceRecordId
	 * 		- the root record ID, that shows which record three the record belongs to
	 * @param parentRecordId
	 * 		- the ID of the predecessor of the record
	 * @param nodeId
	 * 		- ID of the node, where this new record is created
	 * @param childRecordCounter
	 * 		- child record are counted from zero according to a predecessor record
	 * @param isItSource
	 * 		- shows whether the node is a source or not
	 * @return a new RecordId object
	 */
	public static RecordId newReplayableRecordId(long sourceRecordId, long parentRecordId, int nodeId, int childRecordCounter, boolean isItSource) {
		RecordId rid = new RecordId();
		if (isItSource) {
			// if the node is source, keep the root record ID, because at sources this method is called when it would not be necessary...
			rid.currentRecordId = sourceRecordId;
			//rid.currentRecordId = parentRecordId;
		} else {
			// simply put the parameters into a string and than hash the string
			StringBuilder sb = new StringBuilder();
			sb.append(parentRecordId);
			sb.append(nodeId);
			sb.append(childRecordCounter);

			rid.currentRecordId = redis.clients.util.MurmurHash.hash64A(sb.toString().getBytes(), 0x5EED);
		}

		//System.err.printf(String.format("NEW ID COMPONENTS: %s, %s, %s and the NEW ID: %s\n", String.valueOf(parentRecordId), String.valueOf(nodeId), String.valueOf(childRecordCounter), rid.currentRecordId));

		if (LOG.isDebugEnabled()) {
			LOG.debug("NEW ID COMPONENTS: {}, {}, {} and the NEW ID: {}", String.valueOf(parentRecordId), String.valueOf(nodeId), String.valueOf(childRecordCounter), rid.currentRecordId);
		}

		// set the root record ID
		rid.sourceRecordId = sourceRecordId;
		return rid;
	}



}
