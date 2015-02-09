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

package org.apache.flink.runtime.event.task;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.network.api.reader.BufferReaderBase;

public class StreamingSuperstep extends TaskEvent {

	protected long id;
	protected BufferReaderBase bufferReader;

	public StreamingSuperstep() {

	}

	public StreamingSuperstep(long id) {
		this.id = id;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(id);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		id = in.readLong();
	}

	public long getId() {
		return id;
	}

	public StreamingSuperstep setBuffer(BufferReaderBase bufferReader) {
		this.bufferReader = bufferReader;
		return this;
	}

	public BufferReaderBase getBuffer() {
		return bufferReader;
	}

}
