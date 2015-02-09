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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;

public class MultiRecordReader<T extends IOReadableWritable> extends MutableRecordReader<T> implements MutableReader<T> {

	MultiBufferReaderBase multiReader;

	public MultiRecordReader(MultiBufferReaderBase reader) {
		super(reader);
		this.multiReader = reader;
	}

	public int nextRecordWithIndex(final T target) throws IOException, InterruptedException {
		boolean hasNext = getNextRecord(target);
		return hasNext ? multiReader.getNextReaderIndex() : -1;
	}

	@Override
	public void clearBuffers() {
		super.clearBuffers();
	}
}

