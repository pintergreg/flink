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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

public class MessageWithHeader<VertexKey, Message>
	implements Serializable{
	private static final long serialVersionUID = 1L;
	
	// Setters, getters
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
	public Integer getChannelId() {
		return channelId;
	}
	public void setChannelId(Integer channelId) {
		this.channelId = channelId;
	}
	public VertexKey[] getSomeRecipients() {
		return someRecipients;
	}
	public void setSomeRecipients(VertexKey[] someRecipients) {
		this.someRecipients = someRecipients;
	}
	public VertexKey getReprVertexOfPartition() {
		return reprVertexOfPartition;
	}
	public void setReprVertexOfPartition(VertexKey reprVertexOfPartition) {
		this.reprVertexOfPartition = reprVertexOfPartition;
	}


	private VertexKey sender;
	private VertexKey[] someRecipients;
	private Message message;
	private Integer channelId = -1;
	private VertexKey reprVertexOfPartition;
	
	@SuppressWarnings("rawtypes")
	public  static TypeInformation<MessageWithHeader> getTypeInfo(
			TypeInformation<?> keyType, TypeInformation<?> msgType) {
		List<PojoField> fields = new ArrayList<PojoField>();
		try {
			fields.add(new PojoField(MessageWithHeader.class.getDeclaredField("sender"), keyType));
			fields.add(new PojoField(MessageWithHeader.class.getDeclaredField("message"), msgType));
			fields.add(new PojoField(MessageWithHeader.class.getDeclaredField("someRecipients"), ObjectArrayTypeInfo.getInfoFor(Array.newInstance(keyType.getTypeClass(), 0).getClass())));
			fields.add(new PojoField(MessageWithHeader.class.getDeclaredField("channelId"), BasicTypeInfo.INT_TYPE_INFO));
			fields.add(new PojoField(MessageWithHeader.class.getDeclaredField("reprVertexOfPartition"), keyType));
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
				+ ", channelId=" + channelId + ", reprVertexOfPartition="
				+ reprVertexOfPartition + "]";
	}
	
}

