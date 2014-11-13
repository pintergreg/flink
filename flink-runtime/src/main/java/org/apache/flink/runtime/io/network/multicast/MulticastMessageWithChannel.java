package org.apache.flink.runtime.io.network.multicast;

import org.apache.flink.api.java.tuple.Tuple2;

public class MulticastMessageWithChannel extends
		Tuple2<Integer, MulticastMessage> {

	public MulticastMessageWithChannel(int channelId, MulticastMessage msg) {
		super(channelId, msg);
	}

	public int getChannel() {
		return f0;
	}

	public MulticastMessage getMulticastMessage() {
		return f1;
	}

}
