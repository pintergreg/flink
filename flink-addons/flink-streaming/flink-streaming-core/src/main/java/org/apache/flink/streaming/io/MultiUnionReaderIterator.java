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

package org.apache.flink.streaming.io;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;

/**
 * A MultiReaderIterator wraps a {@link MultiRecordReader} producing records of
 * many inputs.
 */
public class MultiUnionReaderIterator<T> implements MultiReaderIterator<T> {

	private MultiRecordReader<DeserializationDelegate<T>> reader;

	protected final DeserializationDelegate<T> delegate;

	public MultiUnionReaderIterator(MultiRecordReader<DeserializationDelegate<T>> reader,
			TypeSerializer<T> serializer) {
		this.reader = reader;
		this.delegate = new DeserializationDelegate<T>(serializer);
	}

	@Override
	public int next(T target) throws IOException {
		delegate.setInstance(target);

		try {
			return this.reader.getNextRecord(delegate);
		} catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}
}