package org.apache.flink.spargel.java.multicast;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

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
	
	public  static TypeInformation<MessageWithSender> getTypeInfo(
			TypeInformation<?> keyType, TypeInformation<?> msgType) {
		List<PojoField> fields = new ArrayList<PojoField>();
		//PojoTypeExtractionTest
		
		//System.out.println(ObjectArrayTypeInfo.getInfoFor(Array.newInstance(keyType.getTypeClass(), 0).getClass()));
		try {
			fields.add(new PojoField(MessageWithSender.class.getField("sender"), keyType));
			fields.add(new PojoField(MessageWithSender.class.getField("message"), msgType));
			fields.add(new PojoField(MessageWithSender.class.getField("someRecipients"), ObjectArrayTypeInfo.getInfoFor(Array.newInstance(keyType.getTypeClass(), 0).getClass())));
			
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TypeInformation<MessageWithSender> res1 = new PojoTypeInfo<MessageWithSender>(MessageWithSender.class, fields);
		return res1;
	}
	@Override
	public String toString() {
		return "MessageWithSender [sender=" + sender + ", someRecipients="
				+ Arrays.toString(someRecipients) + ", message=" + message
				+ "]";
	}
	
}

