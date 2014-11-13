package org.apache.flink.runtime.io.network.multicast;

//TODO: write unit test for this class if possible!!!

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class MulticastMessageBlocker {
	private List<MulticastMessage> storedMessages;
	private Map<Integer, ArrayList<Long>> blockedTargetKeys;
	private SerializationDelegate<MulticastMessage> delegate;
	private Double value;

	public MulticastMessageBlocker(TypeSerializer<MulticastMessage> serializer) {
		this.storedMessages = new ArrayList<MulticastMessage>();
		this.blockedTargetKeys = new HashMap<Integer, ArrayList<Long>>();
		this.delegate = new SerializationDelegate<MulticastMessage>(serializer);
	}

	public void addMessage(MulticastMessage newMessage) {
		storedMessages.add(newMessage);
		// TODO: check if value was preset: here we can check whether the value
		// is the same.. suppose that the first one was the original
		value = newMessage.f1;
	}

	public MulticastMessageWithChannel[] executeMessageBlocking(
			ChannelSelector<SerializationDelegate<MulticastMessage>> selector,
			int numberOfOutputChannels) {
		blockedTargetKeys.clear();

		int[] recordHash = new int[0];
		for (MulticastMessage i : storedMessages) {
			delegate.setInstance(i);
			recordHash = selector.selectChannels(delegate,
					numberOfOutputChannels);
			if (recordHash.length > 1) {
				throw new RuntimeException(
						"There are multiple channels returned instead of 1!");
			} else {
				if (!blockedTargetKeys.containsKey(recordHash[0])) {
					blockedTargetKeys.put(recordHash[0], new ArrayList<Long>());
				}

				if (i.isBlockedMessage()) {
					// TODO: create custom exception object
					throw new RuntimeException(
							"MulticastMessage should not be blocked!");
				} else {
					// add targetId to formerly blocked one with the same
					// channelId
					blockedTargetKeys.get(recordHash[0]).add(i.f0[0]);
				}
			}
		}
		
		//The original messages are deleted. There is no further need for them.
		storedMessages.clear();
		MulticastMessageWithChannel[] blockedMessagesWithChannel = new MulticastMessageWithChannel[blockedTargetKeys
				.size()];

		int index = 0;
		for (int targetChannel : blockedTargetKeys.keySet()) {
			long[] blockedTargets = new long[blockedTargetKeys.get(
					targetChannel).size()];

			int idx = 0;
			for (long i : blockedTargetKeys.get(targetChannel)) {
				blockedTargets[idx] = i;
				idx++;
			}

			blockedMessagesWithChannel[index] = new MulticastMessageWithChannel(
					targetChannel, new MulticastMessage(blockedTargets,
							this.value));

			// blockedMessagesWithChannel[index] = new
			// MulticastMessageWithChannel(
			// targetChannel, new MulticastMessage(blockedTargetKeys.get(
			// targetChannel).toArray(blockedTargets), this.value));

			index++;
			// TODO: check boundary for index
		}
		return blockedMessagesWithChannel;

	}

}
