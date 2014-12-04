package org.apache.flink.spargel.java.multicast;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

public class MessageWithHeader<VertexKey, Message>
	implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public VertexKey getSender() {
		return sender;
	}
	public void setSender(VertexKey sender) {
		this.sender = sender;
	}
	public Message getMessage() {
		return message;
	}
	public void setMessage(Message message) {
		this.message = message;
	}
	
	public VertexKey sender;
	public VertexKey[] someRecipients;
	public Message message;
	public Integer channelId = -1;
	
	@SuppressWarnings("rawtypes")
	public  static TypeInformation<MessageWithHeader> getTypeInfo(
			TypeInformation<?> keyType, TypeInformation<?> msgType) {
		List<PojoField> fields = new ArrayList<PojoField>();
		try {
			fields.add(new PojoField(MessageWithHeader.class.getField("sender"), keyType));
			fields.add(new PojoField(MessageWithHeader.class.getField("message"), msgType));
			fields.add(new PojoField(MessageWithHeader.class.getField("someRecipients"), ObjectArrayTypeInfo.getInfoFor(Array.newInstance(keyType.getTypeClass(), 0).getClass())));
			fields.add(new PojoField(MessageWithHeader.class.getField("channelId"), BasicTypeInfo.INT_TYPE_INFO));
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("No such field!", e);
			//e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Security exception!", e);
			//e.printStackTrace();
		}
		TypeInformation<MessageWithHeader> res1 = new PojoTypeInfo<MessageWithHeader>(MessageWithHeader.class, fields);
		return res1;
	}
	@Override
	public String toString() {
		return "MessageWithHeader [sender=" + sender + ", someRecipients="
				+ Arrays.toString(someRecipients) + ", message=" + message
				+ ", channelId=" + channelId + "]";
	}
	
}

