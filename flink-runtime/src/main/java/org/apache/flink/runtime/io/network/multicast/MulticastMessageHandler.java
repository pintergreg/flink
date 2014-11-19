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

package org.apache.flink.runtime.io.network.multicast;

import java.util.ArrayList;
import java.util.List;

public class MulticastMessageHandler {
	private List<MulticastMessage> storedMessages;
	private int index;

	public MulticastMessageHandler() {
		this.storedMessages = new ArrayList<MulticastMessage>();
		index = 0;
	}

	public boolean hasNext() {
		return index < storedMessages.size();
	}

	// returns an original unblocked message
	public MulticastMessage next() {
		MulticastMessage handedMessage = storedMessages.get(index);
		index++;
		return handedMessage;
	}

	public void unblockMessage(MulticastMessage blockedMessage) {
		// TODO: check whether the value is not null needed?
		if (blockedMessage.f0.length == 0) {
			throw new RuntimeException("The blocked MulticastMessage is empty!");
		} else {
			storedMessages.clear();
			this.index = 0;

			// fill up the storedMessages with the original unblocked
			// messages
			for (long targetId : blockedMessage.f0) {
				storedMessages.add(new MulticastMessage(
						new long[] { targetId }, blockedMessage.f1));
			}
		}
	}

}
