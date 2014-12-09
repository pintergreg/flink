package org.apache.flink.spargel.java.multicast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
		
	}
}
