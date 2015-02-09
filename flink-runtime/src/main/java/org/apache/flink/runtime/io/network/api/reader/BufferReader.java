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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.util.event.EventNotificationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class BufferReader implements BufferReaderBase {

	private static final Logger LOG = LoggerFactory.getLogger(BufferReader.class);

	private final EventNotificationHandler<TaskEvent> taskEventHandler = new EventNotificationHandler<TaskEvent>();

	private boolean isIterativeReader;

	private int currentNumEndOfSuperstepEvents;

	private int channelIndexOfLastReadBuffer = -1;

	private final InputGate gate;

	public BufferReader(InputGate gate) {
		this.gate = checkNotNull(gate);
	}

	@Override
	public void requestPartitionsOnce() throws IOException {
		gate.requestPartitionsOnce();
	}

	@Override
	public Buffer getNextBufferBlocking() throws IOException, InterruptedException {

		requestPartitionsOnce();

		final BufferOrEvent bufferOrEvent = gate.getNextBufferOrEvent();

		final Buffer buffer = bufferOrEvent.getBuffer();

		if (buffer != null) {
			channelIndexOfLastReadBuffer = bufferOrEvent.getChannelIndex();

			return buffer;
		}
		else {
			try {
				final AbstractEvent event = bufferOrEvent.getEvent();

				// ------------------------------------------------------------
				// Runtime events
				// ------------------------------------------------------------
				// Note: We can not assume that every channel will be finished
				// with an according event. In failure cases or iterations the
				// consumer task finishes earlier and has to release all
				// resources.
				// ------------------------------------------------------------
				if (event.getClass() == EndOfPartitionEvent.class) {
					gate.closeInputChannel(bufferOrEvent.getChannelIndex());
					return null;
				}
				else if (event.getClass() == EndOfSuperstepEvent.class) {
					incrementEndOfSuperstepEventAndCheck();

					return null;
				}
				// ------------------------------------------------------------
				// Task events (user)
				// ------------------------------------------------------------
				else if (event instanceof TaskEvent) {
					taskEventHandler.publish((TaskEvent) event);

					return null;
				}
				else {
					throw new IllegalStateException("Received unexpected event " + event + " from input channel.");
				}
			}
			catch (Throwable t) {
				throw new IOException("Error while reading event: " + t.getMessage(), t);
			}
		}
	}

	@Override
	public Buffer getNextBuffer(Buffer exchangeBuffer) {
		throw new UnsupportedOperationException("Buffer exchange when reading data is not yet supported.");
	}

	@Override
	public int getChannelIndexOfLastBuffer() {
		return channelIndexOfLastReadBuffer;
	}

	@Override
	public int getNumberOfInputChannels() {
		return gate.getNumberOfInputChannels();
	}

	@Override
	public boolean isFinished() {
		return gate.isFinished();
	}

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	@Override
	public void subscribeToReader(EventListener<BufferReaderBase> listener) {
		throw new UnsupportedOperationException("Union not supported yet, Gyula. :D");
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		gate.sendTaskEvent(event);
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(listener, eventType);
	}

	// ------------------------------------------------------------------------
	// Iteration end of superstep events
	// ------------------------------------------------------------------------

	@Override
	public void setIterativeReader() {
		isIterativeReader = true;
	}

	@Override
	public void startNextSuperstep() {
		checkState(isIterativeReader, "Tried to start next superstep in a non-iterative reader.");
		checkState(currentNumEndOfSuperstepEvents == gate.getNumberOfInputChannels(),
				"Tried to start next superstep before reaching end of previous superstep.");

		currentNumEndOfSuperstepEvents = 0;
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return currentNumEndOfSuperstepEvents == gate.getNumberOfInputChannels();
	}

	private boolean incrementEndOfSuperstepEventAndCheck() {
		checkState(isIterativeReader, "Received end of superstep event in a non-iterative reader.");

		currentNumEndOfSuperstepEvents++;

		checkState(currentNumEndOfSuperstepEvents <= gate.getNumberOfInputChannels(),
				"Received too many (" + currentNumEndOfSuperstepEvents + ") end of superstep events.");

		return currentNumEndOfSuperstepEvents == gate.getNumberOfInputChannels();
	}

	@Override
	public String toString() {
		return String.format("BufferReader %s", gate.getConsumedResultId());
	}
}