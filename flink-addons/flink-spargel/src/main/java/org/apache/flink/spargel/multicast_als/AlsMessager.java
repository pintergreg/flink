package org.apache.flink.spargel.multicast_als;

import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.types.NullValue;

public final class AlsMessager extends MessagingFunction<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, NullValue> {
	
	private AlsCustomMessageForSpargel msg = new AlsCustomMessageForSpargel();
	
	@Override
	public void sendMessages(Integer vertexId, DoubleVectorWithMap value) {
		msg = new AlsCustomMessageForSpargel(value.getId(), value.getData());;
		sendMessageToAllNeighbors(msg);
	}
}
