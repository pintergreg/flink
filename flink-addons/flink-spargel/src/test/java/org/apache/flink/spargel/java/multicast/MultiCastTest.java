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

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.junit.Test;

@SuppressWarnings({"serial"})
public class MultiCastTest {

	
	//This AtomicInteger is needed because of the concurrent changes of this value
	static AtomicInteger numOfMessagesToSend;
	//We use ConcurrentHashMap, though there was no problem with the HashMap either
	static Map<Tuple2<Long, Long>, Boolean>  messageReceivedAlready = new ConcurrentHashMap<Tuple2<Long, Long>, Boolean>();
	//This AtomicInteger is needed because of the concurrent changes of this value
	static AtomicInteger numOfBlockedMessagesToSend;
	
	//Assume we have at least 2 cores
	public static int degreeOfParalellism = 2; 
	
	@Test
	public void multicastSimpleTest() throws Exception {
		System.out.println("multicastSimpleTest");
		// some input data
		int numOfNodes = 3;
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		edgeList.add(new Tuple2<Long, Long>(0L, 0L));
		edgeList.add(new Tuple2<Long, Long>(0L, 1L));
		edgeList.add(new Tuple2<Long, Long>(0L, 2L));
		// edgeList.add(new Tuple2<Long, Long>(0L, 3L));
		// edgeList.add(new Tuple2<Long, Long>(1L, 2L));
		// edgeList.add(new Tuple2<Long, Long>(3L, 1L));
		// edgeList.add(new Tuple2<Long, Long>(3L, 2L));
		int expectedNumOfBlockedMessages = 2;


		// testing multicast 0: messages are not blocked
		testMulticast(numOfNodes, edgeList, edgeList.size(), edgeList.size(), 0, 0);

		
		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 1, 0);

		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 2, 0);
		
		// testing multicast 2 without blocking the messages
		testMulticast(numOfNodes, edgeList, edgeList.size(), edgeList.size(), 2, 1);

		//testing multicast 2 sendMessageToMultipleRecipients
		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 2, 2);

		// testing multicast 2 withValuedEdges
		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 2, 3);


	}

	@Test
	public void multicastStressTest() throws Exception {
		System.out.println("multicastStressTest");
		int numOfNodes = 100;
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		for (int i = 0; i < numOfNodes; ++i) {
			for (int j = 0; j < numOfNodes; ++j) {
				if (i != j) {
					edgeList.add(new Tuple2<Long, Long>((long) i, (long) j));
				}
			}
		}

		int expectedNumOfBlockedMessages = degreeOfParalellism * numOfNodes;

		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 1, 0);

		testMulticast(numOfNodes, edgeList, edgeList.size(), expectedNumOfBlockedMessages, 2, 0);

	}

	private static void testMulticast(int numOfNodes,
			List<Tuple2<Long, Long>> edgeList, 
			int expectedNumOfMessages, int expectedNumOfBlockedMessages,
			int whichMulticast, int subTestId) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		messageReceivedAlready.clear();
		for (Tuple2<Long, Long> e : edgeList) {
			messageReceivedAlready.put(e, false);
		}
		//numOfMessagesToSend = edgeList.size();
		numOfMessagesToSend = new AtomicInteger(expectedNumOfMessages);
		numOfBlockedMessagesToSend = new AtomicInteger(expectedNumOfBlockedMessages);

		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);

		DataSet<Tuple2<Long, VertexVal>> initialVertices = vertexIds
				.map(new IdAssigner());


		DataSet<Tuple2<Long, VertexVal>> result = null;
		if (whichMulticast == 0) {
			if (subTestId == 0) {
				VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
						.withPlainEdges(edges, new TestUpdater(),
								new TestMessager0(MCEnum.MC0), 1);
				result = initialVertices.runOperation(iteration);
			} else {
				throw new RuntimeException("For multicast 0 the subtest id should be 0");
			}
		} else if (whichMulticast == 1) {
			if (subTestId == 0) {
				VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
						.withPlainEdges(edges, new TestUpdater(),
								new TestMessager1(MCEnum.MC1), 1);
				result = initialVertices.runOperation(iteration);
			} else {
				throw new RuntimeException("For multicast 1 the subtest id should be 0");
			}
		} else if (whichMulticast == 2) {
			if (subTestId == 0) {
				VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
						.withPlainEdges(edges, new TestUpdater(),
								new TestMessager2(MCEnum.MC2), 1);
				result = initialVertices.runOperation(iteration);
			} else if (subTestId == 1) {
				VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
						.withPlainEdges(edges, new TestUpdater(),
								new TestMessager2SendMessageTo(MCEnum.MC2), 1);
				result = initialVertices.runOperation(iteration);
			} else if (subTestId == 2) {
				VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
						.withPlainEdges(edges, new TestUpdater(),
								new TestMessager2SendMessageToMultipleRecipients(MCEnum.MC2), 1);
				result = initialVertices.runOperation(iteration);
			} else if (subTestId == 3) {
				//throw new UnsupportedOperationException("withValuedEdges not yet implemented in MC2");
				//edgesWithValue.print();
				DataSet<Tuple3<Long, Long, EdgeVal>> edgesWithValue = edges.map(
						new MapFunction<Tuple2<Long,Long>, Tuple3<Long, Long, EdgeVal>>() {
							Tuple3<Long, Long, EdgeVal> reuse = new Tuple3<Long, Long, EdgeVal>(0L, 0L, new EdgeVal());
						@Override
						public Tuple3<Long, Long, EdgeVal> map(
								Tuple2<Long, Long> value) throws Exception {
							reuse.f0 = value.f0;
							reuse.f1 = value.f1;
							return reuse;
						}
					});

				VertexCentricIteration<Long, VertexVal, Message, EdgeVal> iteration = VertexCentricIteration
						.withValuedEdges(edgesWithValue, new TestUpdater(),
								new TestMessager2ValuedEdges(MCEnum.MC2), 1);
				result = initialVertices.runOperation(iteration);
			} else {
				throw new RuntimeException("For multicast 2 the subtest id should be 0, 1, 2, 3");
			}
		} else {
			throw new RuntimeException("The value of <whichMulticast>  should be 0, 1, or 2");
		}

		result.max(0).print();
		env.setDegreeOfParallelism(degreeOfParalellism);
		env.execute("Spargel Multiple recipients test.");
		//System.out.println(env.getExecutionPlan());
		
		checkMessages(whichMulticast, subTestId);
		
		// The next line failed to print the error message for me once (but it still failed when it had to)
		assertEquals("The number of blocked messages was not correct"
					+ " (remaining: " + numOfBlockedMessagesToSend.get()
					+ ")", 0, numOfBlockedMessagesToSend.get() );

	}


	private static void checkMessages(int whichMulticast, int subTestId) {
		if (numOfMessagesToSend.get() != 0) {
			int remaining = numOfMessagesToSend.get();
			for (Tuple2<Long, Long> e : messageReceivedAlready.keySet()) {
				if (!messageReceivedAlready.get(e)) {
					System.err.println("Message for edge " + e
							+ " was not delivered.");
					remaining--;
				}
			}
			if (remaining != 0) {
				System.err
						.println("numOfMessagesToSend and messageReceivedAlready are not in sync ("
								+ remaining
								+ " vs "
								+ numOfMessagesToSend
								+ ")");
			}
			throw new RuntimeException("Not every message was delivered in "
					+ whichMulticast + " (remaining: " + numOfMessagesToSend.get()
					+ ", subTestId: " + subTestId + ")");
		} else {
			System.out.println("All messages received in multicast " + whichMulticast + " (subTestId: " + subTestId + ")");
		}
	}
	
	public static final class VertexVal {
		public Integer dummy = 1;
	}

	public static final class EdgeVal implements Serializable{
		public Integer dummy = 3;
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

	

	public static final class TestUpdater extends VertexUpdateFunction<Long, VertexVal, Message> {
		@Override
		public void updateVertex(Long vertexKey, VertexVal vertexValue, MessageIterator<Message> inMessages) {
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
						numOfMessagesToSend.decrementAndGet();
					}
				}
			}
			setNewVertexValue(vertexValue);
		}
	}

	public static final class TestMessager0 extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		public TestMessager0(MCEnum whichMulticast) {
			super(whichMulticast);
		}

		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);
			
			int numOfBlockedMessages = sendMessageToAllNeighbors(m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
			
		}
	}

	public static final class TestMessager1 extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		public TestMessager1(MCEnum whichMulticast) {
			super(whichMulticast);
		}
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);
			
			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				recipients.addRecipient(edge.target());
			}
			int numOfBlockedMessages = sendMessageToMultipleRecipients(recipients, m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
			
		}
	}


	public static final class TestMessager2 extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		public TestMessager2(MCEnum whichMulticast) {
			super(whichMulticast);
		}

		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);

			int numOfBlockedMessages = sendMessageToAllNeighbors(m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
			
			
		}
	}

	public static final class TestMessager2SendMessageTo extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		public TestMessager2SendMessageTo(MCEnum whichMulticast) {
			super(whichMulticast);
		}

		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				numOfBlockedMessagesToSend.addAndGet(-sendMessageTo(edge.target(), m));
			}
		}
	}

	public static final class TestMessager2SendMessageToMultipleRecipients extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		public TestMessager2SendMessageToMultipleRecipients(
				MCEnum whichMulticast) {
			super(whichMulticast);
		}

		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);
			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				recipients.addRecipient(edge.target());
			}
			int numOfBlockedMessages = sendMessageToMultipleRecipients(recipients, m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
		}
	}

	public static final class TestMessager2ValuedEdges extends MessagingFunction<Long, VertexVal, Message, EdgeVal> {
		public TestMessager2ValuedEdges(MCEnum whichMulticast) {
			super(whichMulticast);
		}

		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);

			int numOfBlockedMessages = sendMessageToAllNeighbors(m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
		}
	}

	/**
	 * A map function that takes a Long value and creates a 2-tuple out of it:
	 * <pre>(Long value) -> (value, value)</pre>
	 */
	public static final class IdAssigner implements MapFunction<Long, Tuple2<Long, VertexVal>> {
		@Override
		public Tuple2<Long, VertexVal> map(Long value) {
			return new Tuple2<Long, VertexVal>(value, new VertexVal());
		}
	}

}
