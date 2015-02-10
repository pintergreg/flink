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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.runtime.event.task.StreamingSuperstep;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.reader.BufferReaderBase;
import org.apache.flink.runtime.io.network.api.reader.UnionBufferReader;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.util.event.EventNotificationHandler;

/**
 * A buffer-oriented reader, which unions multiple {@link BufferReader}
 * instances.
 */
public class StreamingUnionReader extends UnionBufferReader {

	Set<BufferReaderBase> readers = new HashSet<BufferReaderBase>();
	Map<Long, Integer> readersAtSuperstep = new HashMap<Long, Integer>();

	private final EventNotificationHandler<StreamingSuperstep> superStepEventHandler = new EventNotificationHandler<StreamingSuperstep>();

	public StreamingUnionReader(BufferReader... readers) {
		super(readers);
		for (BufferReader reader : readers) {
			reader.setReleaseAtSuperstep(false);
		}
		subscribeToSuperstepEvents(new SuperstepBarrierListener());
	}

	private class SuperstepBarrierListener implements EventListener<StreamingSuperstep> {

		@Override
		public void onEvent(StreamingSuperstep event) {
			Integer readersAtThisSuperstep = readersAtSuperstep.get(event.getId());
			if (readersAtThisSuperstep == null) {
				readersAtThisSuperstep = 1;
			} else {
				readersAtThisSuperstep++;
			}
			readersAtSuperstep.put(event.getId(), readersAtThisSuperstep);

			if (readersAtThisSuperstep == readers.size()) {
				superStepEventHandler.publish(event);
			}
		}

	}

	public void subscribeToSuperstepEvents(EventListener<StreamingSuperstep> listener) {
		superStepEventHandler.subscribe(listener, StreamingSuperstep.class);
	}

}
