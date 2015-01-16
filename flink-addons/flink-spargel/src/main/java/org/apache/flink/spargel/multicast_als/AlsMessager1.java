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
