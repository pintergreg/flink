package org.apache.flink.spargel.java.multicast;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.Test;

public class MessageWithHeaderTest {

	@Test
	public void testTypeInfo() {
		@SuppressWarnings("rawtypes")
		TypeInformation<MessageWithHeader> typeInfo = MessageWithHeader
				.getTypeInfo(BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.DOUBLE_TYPE_INFO);
		assertEquals(4, typeInfo.getArity());
		assertFalse(typeInfo.isBasicType());
		assertFalse(typeInfo.isTupleType());
		assertEquals(MessageWithHeader.class, typeInfo.getTypeClass());
		
		
		//we test whether serialization/deserialization works
		MessageWithHeader<Long, Double> record = new MessageWithHeader<Long, Double>();
		Long sender = 1L;
		Integer channel = 0;
		Double message = 0.5;
		Long[] recipients = new Long[]{2L, 3L};
		
		record.setSender(sender);
		record.setChannelId(channel);
		record.setMessage(message);
		record.setSomeRecipients(recipients);
		
		@SuppressWarnings("rawtypes")
		TypeSerializer<MessageWithHeader> serializer = typeInfo.createSerializer();

		
    	@SuppressWarnings("unchecked")
		MessageWithHeader<Long, Double> record2 = serializer.copy(record);
		assertEquals(sender, record2.getSender());
		assertEquals(channel, record2.getChannelId());
		assertEquals(message, record2.getMessage());
		assertArrayEquals(recipients, record2.getSomeRecipients());
    	
		
		
	}
}
