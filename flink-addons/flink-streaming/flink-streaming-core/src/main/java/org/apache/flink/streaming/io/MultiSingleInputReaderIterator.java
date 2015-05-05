/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.io;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;

public class MultiSingleInputReaderIterator<T> implements MultiReaderIterator<T> {

	private final MutableRecordReader<DeserializationDelegate<T>> reader;

	protected final DeserializationDelegate<T> delegate;

	public MultiSingleInputReaderIterator(MutableRecordReader<DeserializationDelegate<T>> reader,
			TypeSerializer<T> serializer) {
		this.reader = reader;
		this.delegate = new ReusingDeserializationDelegate<T>(serializer);
	}

	@Override
	public int nextWithIndex(T target) throws IOException {
		delegate.setInstance(target);

		try {
			if (reader.next(delegate)) {
				return 0;
			} else {
				return -1;
			}

		} catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}
}