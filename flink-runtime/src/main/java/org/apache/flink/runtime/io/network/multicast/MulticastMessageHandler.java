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
