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

package org.apache.flink.streaming.api.streamrecord;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;

import java.io.Serializable;

/**
 * Object for wrapping a tuple or other object with ID used for sending records
 * between streaming task in Apache Flink stream processing.
 */
public class StreamRecord<T> implements IdentifiableStreamRecord, Serializable {
	private static final long serialVersionUID = 1L;

	protected RecordId id;
	protected T streamObject;
	public boolean isTuple;

	/**
	 * Creates an empty StreamRecord
	 */
	public StreamRecord() {
		id = new RecordId();
	}

	/**
	 * @return The ID of the object
	 */
	@Override
	public RecordId getId() {
		return id;
	}

	/**
	 * Sets the ID of the StreamRecord
	 *
	 * @param id
	 * 		id to set
	 */
	@Override
	public void setId(RecordId id) {
		this.id = id;
	}


//	@Override
//	public RecordId newId(long sourceRecordId) {
//
//		id = RecordId.newRecordId(sourceRecordId);
//
//		return id;
//	}

	@Override
	public RecordId newId(long sourceRecordId, long parentRecordId, int instanceID, int childRecordCounter) {

		//TODO ###ID_GEN -- ez a gyermek rekord, adjuk oda neki a szülő ID-jét, meg a nodeID-t
		id = RecordId.newReplayableRecordId(sourceRecordId, parentRecordId, instanceID, childRecordCounter);

		return id;
	}

	/**
	 * Gets the wrapped object from the StreamRecord
	 *
	 * @return The object wrapped
	 */
	public T getObject() {
		return streamObject;
	}

	/**
	 * Gets the field of the contained object at the given position. If a tuple
	 * is wrapped then the getField method is invoked. If the StreamRecord
	 * contains and object of Basic types only position 0 could be returned.
	 *
	 * @param pos
	 * 		Position of the field to get.
	 * @return Returns the object contained in the position.
	 */
	public Object getField(int pos) {
		if (isTuple) {
			return ((Tuple) streamObject).getField(pos);
		} else {
			if (pos == 0) {
				return streamObject;
			} else {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	/**
	 * Extracts key for the stored object using the keySelector provided.
	 *
	 * @param keySelector
	 * 		KeySelector for extracting the key
	 * @return The extracted key
	 */
	public <R> R getKey(KeySelector<T, R> keySelector) {
		try {
			return keySelector.getKey(streamObject);
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract key: " + e.getMessage());
		}
	}

	/**
	 * Sets the object stored
	 *
	 * @param object
	 * 		Object to set
	 * @return Returns the StreamRecord object
	 */
	public StreamRecord<T> setObject(T object) {
		this.streamObject = object;
		return this;
	}

	@Override
	public String toString() {
		return streamObject.toString();
	}

}
