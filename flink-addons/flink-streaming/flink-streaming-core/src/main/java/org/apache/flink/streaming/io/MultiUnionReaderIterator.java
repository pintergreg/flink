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
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;

/**
 * A MultiReaderIterator wraps a {@link MultiBufferReaderBase} producing records of
 * many inputs.
 */
public class MultiUnionReaderIterator<T> implements MultiReaderIterator<T> {

	private final MultiRecordReader<DeserializationDelegate<T>> reader;   // the source

	private final ReusingDeserializationDelegate<T> reusingDelegate;
	private final NonReusingDeserializationDelegate<T> nonReusingDelegate;

	/**
	 * Creates a new iterator, wrapping the given reader.
	 *
	 * @param reader
	 * 		The reader to wrap.
	 */
	public MultiUnionReaderIterator(MultiRecordReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer) {
		this.reader = reader;
		this.reusingDelegate = new ReusingDeserializationDelegate<T>(serializer);
		this.nonReusingDelegate = new NonReusingDeserializationDelegate<T>(serializer);
	}

	@Override
	public int nextWithIndex(T target) throws IOException {
		this.reusingDelegate.setInstance(target);
		int returnIndex;
		try {
			returnIndex = this.reader.nextRecordWithIndex(this.reusingDelegate);

			if (returnIndex != -1) {
				this.reusingDelegate.getInstance();
			}

			return returnIndex;
		} catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}
}