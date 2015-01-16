/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		assertEquals(5, typeInfo.getArity());
		assertFalse(typeInfo.isBasicType());
		assertFalse(typeInfo.isTupleType());
		assertEquals(MessageWithHeader.class, typeInfo.getTypeClass());
		
		
		//we test whether serialization/deserialization works
		MessageWithHeader<Long, Double> record = new MessageWithHeader<Long, Double>();
		Long sender = 1L;
		Integer channel = 0;
		Double message = 0.5;
		Long[] recipients = new Long[]{2L, 3L};
		Long reprVertexOfPartition = 2L;
		
		record.setSender(sender);
		record.setChannelId(channel);
		record.setMessage(message);
		record.setSomeRecipients(recipients);
		record.setReprVertexOfPartition(reprVertexOfPartition);

		
		@SuppressWarnings("rawtypes")
		TypeSerializer<MessageWithHeader> serializer = typeInfo.createSerializer();

		
    	@SuppressWarnings("unchecked")
		MessageWithHeader<Long, Double> record2 = serializer.copy(record);
		assertEquals(sender, record2.getSender());
		assertEquals(channel, record2.getChannelId());
		assertEquals(message, record2.getMessage());
		assertArrayEquals(recipients, record2.getSomeRecipients());
		assertEquals(reprVertexOfPartition, record2.getReprVertexOfPartition());
    	
		
		
	}
}
