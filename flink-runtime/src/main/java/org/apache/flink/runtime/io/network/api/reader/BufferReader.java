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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.StreamingSuperstep;
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

public final class BufferReader implements BufferReaderBase {

	private static final Logger LOG = LoggerFactory.getLogger(BufferReader.class);

	private final EventNotificationHandler<TaskEvent> taskEventHandler = new EventNotificationHandler<TaskEvent>();

	private boolean isIterativeReader;

	private int currentNumEndOfSuperstepEvents;

	private int channelIndexOfLastReadBuffer = -1;

	private final InputGate gate;

	// ----------------------------------------------------------------------------

	private final EventNotificationHandler<StreamingSuperstep> superstepHandler = new EventNotificationHandler<StreamingSuperstep>();

	private boolean releaseAfterSuperstepEvent = true;

	private boolean releaseBarrier = true;

	private final BarrierBuffer barrierBuffer = new BarrierBuffer();

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

		BufferOrEvent bufferOrEvent = null;

		if (barrierBuffer.containsNonprocessed()) {
			bufferOrEvent = barrierBuffer.getNonProcessed();
		}
		while (bufferOrEvent == null) {
			BufferOrEvent nextBufferOrEvent = gate.getNextBufferOrEvent();
			if (barrierBuffer.isBlocked(nextBufferOrEvent)) {
				barrierBuffer.store(nextBufferOrEvent);
			} else {
				bufferOrEvent = nextBufferOrEvent;
			}
		}

		final Buffer buffer = bufferOrEvent.getBuffer();

		if (buffer != null) {
			channelIndexOfLastReadBuffer = bufferOrEvent.getChannelIndex();

			return buffer;
		} else {
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
				} else if (event.getClass() == EndOfSuperstepEvent.class) {
					incrementEndOfSuperstepEventAndCheck();

					return null;
				}

				else if (event.getClass() == StreamingSuperstep.class) {
					StreamingSuperstep superstep = (StreamingSuperstep) event;
					if (!barrierBuffer.receivedSuperstep()) {
						barrierBuffer.startSuperstep(superstep);
					}
					barrierBuffer.blockChannel(bufferOrEvent);
					
					return null;
				}

				// ------------------------------------------------------------
				// Task events (user)
				// ------------------------------------------------------------
				else if (event instanceof TaskEvent) {
					taskEventHandler.publish((TaskEvent) event);

					return null;
				} else {
					throw new IllegalStateException("Received unexpected event " + event
							+ " from input channel.");
				}
			} catch (Throwable t) {
				throw new IOException("Error while reading event: " + t.getMessage(), t);
			}
		}
	}

	@Override
	public Buffer getNextBuffer(Buffer exchangeBuffer) {
		throw new UnsupportedOperationException(
				"Buffer exchange when reading data is not yet supported.");
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
	public void subscribeToTaskEvent(EventListener<TaskEvent> listener,
			Class<? extends TaskEvent> eventType) {
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
				"Received too many (" + currentNumEndOfSuperstepEvents
						+ ") end of superstep events.");

		return currentNumEndOfSuperstepEvents == gate.getNumberOfInputChannels();
	}

	// ------------------------------------------------------------------------
	// Handling streaming supersteps
	// ------------------------------------------------------------------------

	public void subscribeToSuperstepEvents(EventListener<StreamingSuperstep> listener) {
		superstepHandler.subscribe(listener, StreamingSuperstep.class);
	}

	public void setReleaseAtSuperstep(boolean autoRelease) {
		this.releaseAfterSuperstepEvent = autoRelease;
		this.releaseBarrier = autoRelease;
	}

	@Override
	public String toString() {
		return String.format("BufferReader %s", gate.getConsumedResultId());
	}

	private class BarrierBuffer {

		private Queue<BufferOrEvent> bufferOrEvents = new LinkedList<BufferOrEvent>();
		private Queue<BufferOrEvent> unprocessed = new LinkedList<BufferOrEvent>();

		private Set<Integer> blockedChannels = new HashSet<Integer>();
		private int totalNumberOfInputChannels = gate.getNumberOfInputChannels();

		private StreamingSuperstep currentSuperstep;
		private boolean receivedSuperstep;

		public void startSuperstep(StreamingSuperstep superstep) {
			this.currentSuperstep = superstep;
			this.receivedSuperstep = true;
		}

		public void store(BufferOrEvent bufferOrEvent) {
			bufferOrEvents.add(bufferOrEvent);
		}

		public BufferOrEvent getNonProcessed() {
			return unprocessed.poll();
		}

		public boolean isBlocked(BufferOrEvent bufferOrEvent) {
			return blockedChannels.contains(bufferOrEvent);
		}

		public boolean containsNonprocessed() {
			return !unprocessed.isEmpty();
		}

		public boolean receivedSuperstep() {
			return receivedSuperstep;
		}

		public void blockChannel(BufferOrEvent bufferOrEvent) {
			Integer channelIndex = bufferOrEvent.getChannelIndex();
			if (!blockedChannels.contains(channelIndex)) {
				blockedChannels.add(channelIndex);
				if (blockedChannels.size() == totalNumberOfInputChannels) {
					superstepHandler.publish(currentSuperstep);
					unprocessed.addAll(bufferOrEvents);
					bufferOrEvents.clear();
					blockedChannels.clear();
					receivedSuperstep = false;
				}
			} else {
				throw new RuntimeException("Tried to block an already blocked channel");
			}
		}

	}
}