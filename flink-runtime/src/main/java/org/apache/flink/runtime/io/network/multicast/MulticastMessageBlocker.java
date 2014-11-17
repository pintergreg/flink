package org.apache.flink.runtime.io.network.multicast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class MulticastMessageBlocker {

	private long[] targetIds;
	private Double value;
	private MulticastMessage originalMessageImitator;
	private Map<Integer, ArrayList<Long>> blockedTargetKeys;
	private SerializationDelegate<MulticastMessage> delegate;

	public MulticastMessageBlocker(TypeSerializer<MulticastMessage> serializer) {
		this.blockedTargetKeys = new HashMap<Integer, ArrayList<Long>>();
		this.delegate = new SerializationDelegate<MulticastMessage>(serializer);
		originalMessageImitator = new MulticastMessage();
	}

	public void setTargetsAndValue(long[] targets, Double value) {
		// TODO: should we set a size constraint for the targets? If too long
		// maybe it should be splitted into more messages
		this.targetIds = targets;
		this.value = value;
	}

	public MulticastMessageWithChannel[] executeMessageBlocking(
			ChannelSelector<SerializationDelegate<MulticastMessage>> selector,
			int numberOfOutputChannels) {
		blockedTargetKeys.clear();
		int[] recordHash = new int[0];

		for (long i : this.targetIds) {
			originalMessageImitator.setFields(new long[] { i }, this.value);
			delegate.setInstance(originalMessageImitator);
			recordHash = selector.selectChannels(delegate,
					numberOfOutputChannels);
			if (recordHash.length > 1) {
				throw new RuntimeException(
						"There are multiple channels returned instead of 1!");
			} else {
				if (!blockedTargetKeys.containsKey(recordHash[0])) {
					blockedTargetKeys.put(recordHash[0], new ArrayList<Long>());
				}

				blockedTargetKeys.get(recordHash[0]).add(i);
			}
		}

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
			index++;
		}
		return blockedMessagesWithChannel;

	}

}
