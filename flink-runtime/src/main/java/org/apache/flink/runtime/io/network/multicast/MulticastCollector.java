/**
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

package org.apache.flink.runtime.io.network.multicast;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;

/**
 * The OutputCollector collects records, and emits the pair to a set of Nephele
 * {@link RecordWriter}s. The OutputCollector tracks to which writers a
 * deep-copy must be given and which not.
 */
public class MulticastCollector implements Collector<MulticastMessage> {
	// list of writers
	protected RecordWriter<SerializationDelegate<MulticastMessage>>[] writers;

	private final TypeSerializer<MulticastMessage> serializer;

	private MulticastMessageBlocker blocker;

	/**
	 * Initializes the output collector with a set of writers. To specify for a
	 * writer that it must be fed with a deep-copy, set the bit in the copy flag
	 * bit mask to 1 that corresponds to the position of the writer within the
	 * {@link List}.
	 * 
	 * @param writers
	 *            List of all writers.
	 */
	@SuppressWarnings("unchecked")
	public MulticastCollector(
			List<RecordWriter<SerializationDelegate<MulticastMessage>>> writers,
			TypeSerializer<MulticastMessage> serializer) {
		this.serializer = serializer;
		this.blocker = new MulticastMessageBlocker(serializer);
		this.writers = (RecordWriter<SerializationDelegate<MulticastMessage>>[]) writers
				.toArray(new RecordWriter[writers.size()]);
	}

	/**
	 * Adds a writer to the OutputCollector.
	 * 
	 * @param writer
	 *            The writer to add.
	 */

	@SuppressWarnings("unchecked")
	public void addWriter(
			RecordWriter<SerializationDelegate<MulticastMessage>> writer) {
		// avoid using the array-list here to reduce one level of object
		// indirection
		if (this.writers == null) {
			this.writers = new RecordWriter[] { writer };
		} else {
			RecordWriter<SerializationDelegate<MulticastMessage>>[] ws = new RecordWriter[this.writers.length + 1];
			System.arraycopy(this.writers, 0, ws, 0, this.writers.length);
			ws[this.writers.length] = writer;
			this.writers = ws;
		}
	}

	/**
	 * Collects a record and emits it to all writers.
	 */

	@Override
	public void collect(MulticastMessage record) {
		//SYSO:
		System.out.println("Inside MulticastCollector collect().");
		
		blocker.setTargetsAndValue(record.f0, record.f1);
		try {
			for (int i = 0; i < writers.length; i++) {
				// The channels are selected for each writer separetaly
				MulticastMessageWithChannel[] blockedMessagesToSend = blocker
						.executeMessageBlocking(
								this.writers[i].getChannelSelector(),
								this.writers[i].getNumChannels());
				// Own SerialiozationDelegate is needed for each blocked message
				SerializationDelegate<MulticastMessage> delegate;
				for (MulticastMessageWithChannel messageWithChannel : blockedMessagesToSend) {
					// SYSO: what does the blocked message look like?
					System.out
							.println(messageWithChannel.getMulticastMessage());

					delegate = new SerializationDelegate<MulticastMessage>(
							this.serializer);
					delegate.setInstance(messageWithChannel
							.getMulticastMessage());
					// Each blocked message is emitted with the pre-computed
					// targetChannel
					this.writers[i].emit(messageWithChannel.getChannel(),
							delegate);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(
					"Emitting the record caused an I/O exception: "
							+ e.getMessage(), e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Emitting the record was interrupted: "
					+ e.getMessage(), e);
		}
	}

	@Override
	public void close() {
		for (RecordWriter<SerializationDelegate<MulticastMessage>> writer : writers) {
			try {
				writer.flush();
			} catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	/**
	 * List of writers that are associated with this output collector
	 * 
	 * @return list of writers
	 */
	public List<RecordWriter<SerializationDelegate<MulticastMessage>>> getWriters() {
		return Collections.unmodifiableList(Arrays.asList(this.writers));
	}
}
