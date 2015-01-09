package org.apache.flink.spargel.multicast_als;

import org.apache.flink.spargel.java.MessagingFunction1;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;
import org.apache.flink.types.NullValue;

public final class AlsMessager1 extends MessagingFunction1<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, NullValue> {
	
	//TODO: what if I use it as private?
	//private AlsCustomMessageForSpargel msg = new AlsCustomMessageForSpargel();
	
	@Override
	public void sendMessages(Integer vertexId, DoubleVectorWithMap value) {
		AlsCustomMessageForSpargel msg = new AlsCustomMessageForSpargel(value.getId(), value.getData());;
		MultipleRecipients<Integer> recipients = new MultipleRecipients<Integer>();
		for (OutgoingEdge<Integer, NullValue> edge : getOutgoingEdges()) {
			recipients.addRecipient(edge.target());
		}
		sendMessageToMultipleRecipients(recipients, msg);
	}
}
