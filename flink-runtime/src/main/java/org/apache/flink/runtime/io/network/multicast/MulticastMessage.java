package org.apache.flink.runtime.io.network.multicast;

import org.apache.flink.api.java.tuple.Tuple2;

public class MulticastMessage extends Tuple2<long[], Double> {

	public MulticastMessage() {
		super();
	}

	public MulticastMessage(long[] targetIds, Double value) {
		super(targetIds, value);
	}

	public boolean isBlockedMessage() {
		return (f0.length > 1);
	}
}
