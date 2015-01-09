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
package org.apache.flink.spargel.multicast_experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction1;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexCentricIteration1;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;


@SuppressWarnings({"serial"})
//@SuppressWarnings({"serial"})
//@SuppressWarnings({"unchecked"})
public class MultipleRecipientsValuedEdgesTestMain {

	
	static List<Set<Long>> inNeighbours;
	static int numOfReceivedMEssages;
	static Map<Tuple2<Long, Long>, Boolean>  messageReceivedAlready;
	
	public static void main(String[] args) throws Exception {
//		MultipleRecipientsTestMain instance = new MultipleRecipientsTestMain();
//
//		instance.run();
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		DataSet<Long> vertexIds = env.generateSequence(0, 10);
//		DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(0L, 2L), new Tuple2<Long, Long>(2L, 4L), new Tuple2<Long, Long>(4L, 8L),
//															new Tuple2<Long, Long>(1L, 5L), new Tuple2<Long, Long>(3L, 7L), new Tuple2<Long, Long>(3L, 9L));
		
//		List<Set<Integer>> inNeighbours = new ArrayList<Set<Integer>>(null); 
//		inNeighbours[0] = new TreeSet<Integer>
//		inNeighbours[0] = new HashSet<String>(Arrays.asList("a", "b"));
		
		inNeighbours  = new ArrayList<Set<Long>>();
		
		//inNeighbours of 0
		inNeighbours.add(new HashSet<Long>(Arrays.asList(0L)));
		//inNeighbours of 1
		inNeighbours.add(new HashSet<Long>(Arrays.asList(0L)));
		//inNeighbours of 2
		inNeighbours.add(new HashSet<Long>(Arrays.asList(0L, 1L)));

		int numOfNodes = inNeighbours.size();
		numOfReceivedMEssages = 0;
		messageReceivedAlready = new HashMap<Tuple2<Long, Long>, Boolean>();
		
		List<Tuple3<Long, Long, Double>> edgeList = new ArrayList<Tuple3<Long, Long, Double>>();
		for (int i = 0; i < numOfNodes; ++i) {
			for (long j:inNeighbours.get(i)) {
				numOfReceivedMEssages++;
				Tuple3<Long, Long, Double> edge = new Tuple3<Long, Long, Double>(j, (long)i, 0.5);
				edgeList.add(edge);
				messageReceivedAlready.put(new Tuple2<Long, Long>(j, (long)i), false);
			}
		}
		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple3<Long, Long, Double>> edges = env.fromCollection(edgeList);
		
		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
		

		
		VertexCentricIteration1<Long, Long, Message, Double> iteration = VertexCentricIteration1.withValuedEdges(edges, new CCUpdater(), new CCMessager(), 1);
		
		DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(iteration);
		
		result.print();
		env.setDegreeOfParallelism(2);
		env.execute("Spargel Multiple recipients test.");
		//System.out.println(env.getExecutionPlan());
//		if (numOfReceivedMEssages != 0) {
//			throw new RuntimeException("Not every message was delivered (remaining: " + numOfReceivedMEssages + ")");
//		}
		
	}
	
	
	public static final class Message {
		public Long senderke;
		
		@Override
		public String toString() {
			return "Message [senderke=" + senderke + "]";
		}
		public Message() {
			senderke = -1L;
		}
		public Message(Long a) {
			this.senderke = a;
		}
	}

	public static final class CCUpdater extends VertexUpdateFunction<Long, Long, Message> {
		@Override
		public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator<Message> inMessages) {
			for (Message msg: inMessages) {
//				System.out.println("Message from " + msg.sender + " to " + vertexKey + " and " + Arrays.toString(msg.someRecipients));
//				System.out.println("Message contents " + msg.message);
//				if (! inNeighbours.get(vertexKey.intValue()).contains(msg.sender)) {
//					throw new RuntimeException("invalid message from " + msg + " to " + vertexKey);
//				} else {
//					numOfReceivedMEssages--;
//				}
				Tuple2<Long, Long> edge = new Tuple2<Long, Long>(msg.senderke, vertexKey);
				if (!messageReceivedAlready.containsKey(edge)) {
					throw new RuntimeException("invalid message from " + msg.senderke + " to " + vertexKey);
				} else {
					if (messageReceivedAlready.get(edge)) {
						throw new RuntimeException("Message from " + msg.senderke
								+ " to " + vertexKey + " sent more than once.");
					} else {
						messageReceivedAlready.put(edge, true);
						numOfReceivedMEssages--;
					}
				}
			}
		}
	}
	
	public static final class CCMessager extends MessagingFunction1<Long, Long, Message, Double> {
		boolean multiRecipients = false;
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			Message m = new Message(vertexId);

			MultipleRecipients<Long> recipients = new MultipleRecipients<Long>();
			for (OutgoingEdge<Long, Double> edge : getOutgoingEdges()) {
				//sendMessageTo(edge.target(), m);
				recipients.addRecipient(edge.target());
			}
//			System.out.println("Sending from "+ vertexId);
//			System.out.println("To "+ recipients);
			sendMessageToMultipleRecipients(recipients, m);
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
