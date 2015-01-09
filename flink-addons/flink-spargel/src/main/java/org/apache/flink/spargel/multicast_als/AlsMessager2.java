package org.apache.flink.spargel.multicast_als;

import org.apache.flink.spargel.java.MessagingFunction2;
import org.apache.flink.types.NullValue;

public final class AlsMessager2 extends MessagingFunction2<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, NullValue> {
	
	//TODO: what if I use it as private?
	//private AlsCustomMessageForSpargel msg = new AlsCustomMessageForSpargel();
	
	@Override
	public void sendMessages(Integer vertexId, DoubleVectorWithMap value) {
		AlsCustomMessageForSpargel msg = new AlsCustomMessageForSpargel(value.getId(), value.getData());;
		sendMessageToAllNeighbors(msg);
	}
}
