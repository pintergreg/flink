package org.apache.flink.spargel.java;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple2;

public class MessageWithSender<VertexKey, Message>
	extends Tuple2<Message, VertexKey>
	implements Serializable{
	private static final long serialVersionUID = 1L;

	public VertexKey getSender() {
		return f1;
	}
	public void setSender(VertexKey sender) {
		this.f1 = sender;
	}
//	public VertexKey[] getSomeRecipients() {
//		return someRecipients;
//	}
//	public void setSomeRecipients(VertexKey[] someRecipients) {
//		this.someRecipients = someRecipients;
//	}
	public Message getMessage() {
		return f0;
	}
	public void setMessage(Message message) {
		this.f0 = message;
	}
	
	private VertexKey sender;
	private VertexKey[] someRecipients;
	private Message message;
	
}

