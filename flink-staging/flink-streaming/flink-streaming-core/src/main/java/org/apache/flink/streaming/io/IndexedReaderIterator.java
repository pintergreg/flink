/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A {@link MutableObjectIterator} that wraps a reader from an input channel and
 * produces the reader's records.
 * 
 * The reader supports reading objects with possible reuse of mutable types, and
 * without reuse of mutable types.
 */
public final class IndexedReaderIterator<T> extends ReaderIterator<T> {

	private IndexedMutableReader<DeserializationDelegate<T>> reader;

	public IndexedReaderIterator(IndexedMutableReader<DeserializationDelegate<T>> reader,
			TypeSerializer<T> serializer) {
		super(reader, serializer);
		this.reader = reader;
	}

	public int getLastChannelIndex() {
		return reader.getLastChannelIndex();
	}
}
