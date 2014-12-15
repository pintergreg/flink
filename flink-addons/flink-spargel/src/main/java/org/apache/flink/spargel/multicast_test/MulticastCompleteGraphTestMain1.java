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
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.MessagingFunction1;
import org.apache.flink.spargel.java.MessagingFunction2;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexCentricIteration1;
import org.apache.flink.spargel.java.VertexCentricIteration2;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;
import org.apache.flink.types.NullValue;

public class MulticastCompleteGraphTestMain1 {

	public static final String NUM_OF_RECEIVED_MESSAGES = "NUM_OF_RECEIVED_MESSAGES";
	
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("Testing spargel multicast on a complete graph." );

		int whichMulticast = 0;
		int numOfNodes = 0;
		int degreeOfParalellism = 0;

		if (args.length == 3) {
			
			// some input data
			whichMulticast = Integer.parseInt(args[0]);
			numOfNodes = Integer.parseInt(args[1]);
			degreeOfParalellism = Integer.parseInt(args[2]);
		} else  if (args.length == 0) {
			// default
			whichMulticast = 1;
			numOfNodes = 10;
			degreeOfParalellism = 2;
			System.out.println(" Running spargel multicast on a complete graph with default parameters");
		} else {
			System.out.println("Usage: <whichMulticast> <numOfNodes> <degreeOfParalellism>" );
			System.out.println("<whichMulticast> is either 0, 1 or 2." );
		}
			
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		for (int i = 0; i < numOfNodes; ++i) {
			for (int j = 0; j < numOfNodes; ++j) {
				if (i != j) {
					edgeList.add(new Tuple2<Long, Long>((long) i, (long) j));
				}
			}
		}

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);

		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);

		DataSet<Tuple2<Long, VertexVal>> initialVertices = vertexIds
				.map(new IdAssigner());

		DataSet<Tuple2<Long, VertexVal>> result = null;

		if (whichMulticast == 0) {
			VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
					.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 1);
			result = initialVertices.runOperation(iteration);
		} else if (whichMulticast == 1) {
			VertexCentricIteration1<Long, VertexVal, Message, ?> iteration = VertexCentricIteration1
					.withPlainEdges(edges, new CCUpdater(), new CCMessager1(),
							1);
			result = initialVertices.runOperation(iteration);
		} else if (whichMulticast == 2) {
			VertexCentricIteration2<Long, VertexVal, Message, ?> iteration = VertexCentricIteration2
					.withPlainEdges(edges, new CCUpdater(), new CCMessager2(),
							1);
			result = initialVertices.runOperation(iteration);
		} else {
			throw new RuntimeException(
					"The value of <whichMulticast>  should be 0, 1, or 2");
		}

		result.sum(0).print();
		env.setDegreeOfParallelism(degreeOfParalellism);
		// System.out.println(env.getExecutionPlan());
		JobExecutionResult jobRes = env
				.execute("Spargel Multiple recipients test with multicast "
						+ whichMulticast);
		Long numOfRecMsgs = jobRes
				.getAccumulatorResult(NUM_OF_RECEIVED_MESSAGES);
		if (numOfRecMsgs != edgeList.size()) {
			throw new RuntimeException("The number of received messages was "
					+ numOfRecMsgs + " instead of " + edgeList.size());
		}
		
	}

	public static final class VertexVal {
		public Integer dummy = 1;
//		public VertexVal(){
//			dummy = new Integer(0);
//		}
	}
	
	public static final class Message {
		public Long senderId;
		public Double[] dummy = new  Double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
		
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

	

	public static final class CCUpdater extends VertexUpdateFunction<Long, VertexVal, Message> {
		private static final long serialVersionUID = 1L;
		
		private LongCounter numOfRecievedMessages = new LongCounter();

		@Override
		public void preSuperstep() throws Exception {
			getRuntimeContext().addAccumulator(MulticastCompleteGraphTestMain1.NUM_OF_RECEIVED_MESSAGES, numOfRecievedMessages);
		}
		
		@Override
		public void updateVertex(Long vertexKey, VertexVal vertexValue, MessageIterator<Message> inMessages) {
			long msgcount = 0;
			for (Message msg: inMessages) {
				System.out.println("Message from " + msg.senderId + " to " + vertexKey);
				msgcount ++;
//				System.out.println("Message contents " + msg);
				
//				Tuple2<Long, Long> edge = new Tuple2<Long, Long>(msg.senderId, vertexKey);
//				if (!messageReceivedAlready.containsKey(edge)) {
//					throw new RuntimeException("invalid message from " + msg.senderId + " to " + vertexKey);
//				} else {
//					if (messageReceivedAlready.get(edge)) {
//						throw new RuntimeException("Message from " + msg.senderId
//								+ " to " + vertexKey + " sent more than once.");
//					} else {
//						messageReceivedAlready.put(edge, true);
//						numOfMessagesToSend.decrementAndGet();
//					}
//				}
			}
			numOfRecievedMessages.add(msgcount);
		}
	}

	public static final class CCMessager extends MessagingFunction<Long, VertexVal, Message, NullValue> {
		private static final long serialVersionUID = 1L;
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);

			sendMessageToAllNeighbors(m);
		}
	}

	public static final class CCMessager1 extends MessagingFunction1<Long, VertexVal, Message, NullValue> {
		private static final long serialVersionUID = 1L;
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
			Message m = new Message(vertexId);

			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				recipients.addRecipient(edge.target());
			}
			sendMessageToMultipleRecipients(recipients, m);
		}
	}


	public static final class CCMessager2 extends MessagingFunction2<Long, VertexVal, Message, NullValue> {
		private static final long serialVersionUID = 1L;
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, VertexVal componentId) {
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
	public static final class IdAssigner implements MapFunction<Long, Tuple2<Long, VertexVal>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, VertexVal> map(Long value) {
			return new Tuple2<Long, VertexVal>(value, new VertexVal());
		}
	}

}
