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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

		testMulticast(numOfNodes, edgeList, expectedNumOfBlockedMessages, 1);

		testMulticast(numOfNodes, edgeList, expectedNumOfBlockedMessages, 2);
	}

	@Test
	public void multicastStressTest() throws Exception {
		int numOfNodes = 100;
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		for (int i = 0; i < numOfNodes; ++i) {
			for (int j = 0; j < numOfNodes; ++j) {
				if (i != j) {
					edgeList.add(new Tuple2<Long, Long>((long) i, (long) j));
				}
			}
		}

		// testMulticast1(numOfNodes, edgeList);
		//
		// testMulticast2(numOfNodes, edgeList);
		
		// two, because the degree of paralellism is 2
		int expectedNumOfBlockedMessages = degreeOfParalellism * numOfNodes;

		testMulticast(numOfNodes, edgeList, expectedNumOfBlockedMessages, 1);

		testMulticast(numOfNodes, edgeList, expectedNumOfBlockedMessages, 2);

	}

	// this and testMulticast2 are essentially the same
	private static void testMulticast(int numOfNodes,
			List<Tuple2<Long, Long>> edgeList, int expectedNumOfBlockedMessages,
			int whichMulticast) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		messageReceivedAlready.clear();
		for (Tuple2<Long, Long> e : edgeList) {
			messageReceivedAlready.put(e, false);
		}
		//numOfMessagesToSend = edgeList.size();
		numOfMessagesToSend = new AtomicInteger(edgeList.size());
		numOfBlockedMessagesToSend = new AtomicInteger(expectedNumOfBlockedMessages);

		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);

		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds
				.map(new IdAssigner());


		DataSet<Tuple2<Long, Long>> result = null;
		if (whichMulticast == 1) {
			VertexCentricIteration1<Long, Long, Message, ?> iteration = VertexCentricIteration1
					.withPlainEdges(edges, new TestUpdater(),
							new TestMessager1(), 1);
			result = initialVertices.runOperation(iteration);
		} else if (whichMulticast == 2) {
			VertexCentricIteration2<Long, Long, Message, ?> iteration = VertexCentricIteration2
					.withPlainEdges(edges, new TestUpdater(),
							new TestMessager2(), 1);
			result = initialVertices.runOperation(iteration);
		} else {
			throw new RuntimeException("The value of <whichMulticast>  should be 1, or 2");
		}

		result.print();
		env.setDegreeOfParallelism(degreeOfParalellism);
		env.execute("Spargel Multiple recipients test.");
		//System.out.println(env.getExecutionPlan());
		
		checkMessages(whichMulticast);
		
		
		assertEquals("The number of blocked messages was not correct"
					+ " (remaining: " + numOfBlockedMessagesToSend.get()
					+ ")", 0, numOfBlockedMessagesToSend.get() );
	}


	private static void checkMessages(int whichMulticast) {
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
					+ ")");
		} else {
			System.out.println("All messages received in multicast " + whichMulticast);
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

	

	public static final class TestUpdater extends VertexUpdateFunction<Long, Long, Message> {
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
						numOfMessagesToSend.decrementAndGet();
					}
				}
			}
		}
	}

	public static final class TestMessager1 extends MessagingFunction1<Long, Long, Message, NullValue> {
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			Message m = new Message(vertexId);
			
			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				recipients.addRecipient(edge.target());
			}
			int numOfBlockedMessages = sendMessageToMultipleRecipients(recipients, m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);
			
		}
	}


	public static final class TestMessager2 extends MessagingFunction2<Long, Long, Message, NullValue> {
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			Message m = new Message(vertexId);

			int numOfBlockedMessages = sendMessageToAllNeighbors(m);
			numOfBlockedMessagesToSend.addAndGet(-numOfBlockedMessages);

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
