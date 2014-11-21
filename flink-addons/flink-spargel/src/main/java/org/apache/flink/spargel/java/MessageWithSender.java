package org.apache.flink.spargel.java;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple2;

public class MessageWithSender<VertexKey, Message>
	implements Serializable{
	private static final long serialVersionUID = 1L;
	
//	public MessageWithSender(){
//	}
//
//	public MessageWithSender(Tuple2<Message, VertexKey> other){
//		this.message = other.message;
//		this.sender = other.sender;
//	}
	
	public VertexKey getSender() {
		return sender;
	}
	public void setSender(VertexKey sender) {
		this.sender = sender;
	}
//	public VertexKey[] getSomeRecipients() {
//		return someRecipients;
//	}
//	public void setSomeRecipients(VertexKey[] someRecipients) {
//		this.someRecipients = someRecipients;
//	}
	public Message getMessage() {
		return message;
	}
	public void setMessage(Message message) {
		this.message = message;
	}
	
	public VertexKey sender;
	public VertexKey[] someRecipients;
	public Message message;

	
}

