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
package org.apache.flink.spargel.multicast_test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction1;
import org.apache.flink.spargel.java.MessagingFunction2;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexCentricIteration1;
import org.apache.flink.spargel.java.VertexCentricIteration2;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;
import org.apache.flink.types.NullValue;


@SuppressWarnings({"serial"})
public class MultipleRecipientsTestMain {

	
	static List<Set<Long>> inNeighbours;
	static int numOfMessagesToSend;
	static Map<Tuple2<Long, Long>, Boolean>  messageReceivedAlready = new HashMap<Tuple2<Long, Long>, Boolean>();
	
	public static void main(String[] args) throws Exception {

		
		//some input data
		int numOfNodes = 4;
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		edgeList.add(new Tuple2<Long, Long>(0L, 0L));
		edgeList.add(new Tuple2<Long, Long>(0L, 1L));
		edgeList.add(new Tuple2<Long, Long>(0L, 2L));
		edgeList.add(new Tuple2<Long, Long>(1L, 2L));
		edgeList.add(new Tuple2<Long, Long>(3L, 1L));
		edgeList.add(new Tuple2<Long, Long>(3L, 2L));

		testMulticast1(numOfNodes, edgeList);
		testMulticast2(numOfNodes, edgeList);
		
	}

	private static void testMulticast1(int numOfNodes,
			List<Tuple2<Long, Long>> edgeList) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		messageReceivedAlready.clear();
		for (Tuple2<Long, Long> e : edgeList) {
			messageReceivedAlready.put(e, false);
		}
		numOfMessagesToSend = edgeList.size();

		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);

		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds
				.map(new IdAssigner());

		VertexCentricIteration1<Long, Long, Message, ?> iteration = VertexCentricIteration1
				.withPlainEdges(edges, new CCUpdater(), new CCMessager1(), 1);

		DataSet<Tuple2<Long, Long>> result = initialVertices
				.runOperation(iteration);

		result.print();
		env.setDegreeOfParallelism(2);
		env.execute("Spargel Multiple recipients test.");
		// System.out.println(env.getExecutionPlan());
		if (numOfMessagesToSend != 0) {
			throw new RuntimeException(
					"Not every message was delivered (remaining: "
							+ numOfMessagesToSend + ")");
		} else {
			System.out.println("All messages received in multicast 1");
		}
	}

	private static void testMulticast2(int numOfNodes,
			List<Tuple2<Long, Long>> edgeList) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		messageReceivedAlready.clear();
		for (Tuple2<Long, Long> e : edgeList) {
			messageReceivedAlready.put(e, false);
		}
		numOfMessagesToSend = edgeList.size();

		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);

		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds
				.map(new IdAssigner());

		VertexCentricIteration2<Long, Long, Message, ?> iteration = VertexCentricIteration2
				.withPlainEdges(edges, new CCUpdater(), new CCMessager2(), 1);

		DataSet<Tuple2<Long, Long>> result = initialVertices
				.runOperation(iteration);

		result.print();
		env.setDegreeOfParallelism(2);
		env.execute("Spargel Multiple recipients test.");
		// System.out.println(env.getExecutionPlan());
		if (numOfMessagesToSend != 0) {
			throw new RuntimeException(
					"Not every message was delivered (remaining: "
							+ numOfMessagesToSend + ")");
		} else {
			System.out.println("All messages received in multicast 2");
		}
	}

	
	
	public static final class Message {
		public Long senderId;
		
		@Override
		public String toString() {
			return "Message [senderke=" + senderId + "]";
		}
		public Message() {
			senderId = -1L;
		}
		public Message(Long a) {
			this.senderId = a;
		}
	}

	

	public static final class CCUpdater extends VertexUpdateFunction<Long, Long, Message> {
		@Override
		public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator<Message> inMessages) {
			for (Message msg: inMessages) {
//				System.out.println("Message from " + msg.senderId + " to " + vertexKey);
//				System.out.println("Message contents " + msg);

				Tuple2<Long, Long> edge = new Tuple2<Long, Long>(msg.senderId, vertexKey);
				if (!messageReceivedAlready.containsKey(edge)) {
					throw new RuntimeException("invalid message from " + msg.senderId + " to " + vertexKey);
				} else {
					if (messageReceivedAlready.get(edge)) {
						throw new RuntimeException("Message from " + msg.senderId
								+ " to " + vertexKey + " sent more than once.");
					} else {
						messageReceivedAlready.put(edge, true);
						numOfMessagesToSend--;
					}
				}
			}
		}
	}

	public static final class CCMessager1 extends MessagingFunction1<Long, Long, Message, NullValue> {
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			Message m = new Message(vertexId);

			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				recipients.addRecipient(edge.target());
			}
			sendMessageToMultipleRecipients(recipients, m);
		}
	}

	public static final class CCMessager2 extends MessagingFunction2<Long, Long, Message, NullValue> {
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			Message m = new Message(vertexId);
//			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
//			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
//				// sendMessageTo(edge.target(), m);
//				recipients.addRecipient(edge.target());
//			}
//			// System.out.println("Sending from "+ vertexId);
//			// System.out.println("To "+ recipients);
//			sendMessageToMultipleRecipients(recipients, m);

			sendMessageToAllNeighbors(m);
		}
	}
	
	/**
	 * A map function that takes a Long value and creates a 2-tuple out of it:
	 * <pre>(Long value) -> (value, value)</pre>
	 */
	public static final class IdAssigner implements MapFunction<Long, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Long value) {
			return new Tuple2<Long, Long>(value, value);
		}
	}

}
