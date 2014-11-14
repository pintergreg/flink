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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A {@link MutableObjectIterator} that wraps a Nephele Reader producing records
 * of a certain type.
 */
public final class MulticastReaderIterator implements
		MutableObjectIterator<MulticastMessage> {

	private final MutableReader<DeserializationDelegate<MulticastMessage>> reader; // the
																					// source

	private final DeserializationDelegate<MulticastMessage> delegate;

	private MulticastMessageHandler handler;

	/**
	 * Creates a new iterator, wrapping the given reader.
	 * 
	 * @param reader
	 *            The reader to wrap.
	 */
	public MulticastReaderIterator(
			MutableReader<DeserializationDelegate<MulticastMessage>> reader,
			TypeSerializer<MulticastMessage> serializer) {
		this.reader = reader;
		this.delegate = new DeserializationDelegate<MulticastMessage>(
				serializer);
		this.handler = new MulticastMessageHandler();
		
		//SYSO:
		System.out.println("Initialize MulticastReaderIterator.");
	}

	public boolean updateHandler() throws IOException {
		MulticastMessage blockMessage = nextBlockedMessage(new MulticastMessage());
		if (blockMessage == null) {
			return false;
		} else {
			handler.unblockMessage(blockMessage);
			return true;
		}
	}

	public MulticastMessage nextBlockedMessage(MulticastMessage target)
			throws IOException {
		this.delegate.setInstance(target);
		try {
			if (this.reader.next(this.delegate)) {
				return this.delegate.getInstance();
			} else {
				return null;
			}

		} catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}

	@Override
	public MulticastMessage next(MulticastMessage target) throws IOException {
		// NOTE: target is unused
		if (!handler.hasNext()) {
			if (!updateHandler()) {
				return null;
			}
		}
		return handler.next();
	}
}
