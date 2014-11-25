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

import java.io.Serializable;
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
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.MessagingFunction2;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexCentricIteration2;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.java.multicast.MessageWithSender;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;
import org.apache.flink.spargel.multicast_test.MultipleRecipientsTestMain.Message;
import org.apache.flink.types.NullValue;


@SuppressWarnings({"serial", "unchecked"})
//@SuppressWarnings({"serial"})
//@SuppressWarnings({"unchecked"})
public class MultiCastToOutneighboursTestMain {

	
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
		
		List<Tuple2<Long, Long>> edgeList = new ArrayList<Tuple2<Long, Long>>();
		for (int i = 0; i < numOfNodes; ++i) {
			for (long j:inNeighbours.get(i)) {
				numOfReceivedMEssages++;
				Tuple2<Long, Long> edge = new Tuple2<Long, Long>(j, (long)i);
				edgeList.add(edge);
				messageReceivedAlready.put(edge, false);
			}
		}
		DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);
		DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeList);
		
		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
		

		
		VertexCentricIteration2<Long, Long,  Message, ?> iteration = VertexCentricIteration2.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 1);
		
		DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(iteration);
		
		result.print();
		env.setDegreeOfParallelism(2);
		env.execute("Spargel Connected Components");
		if (numOfReceivedMEssages != 0) {
			throw new RuntimeException("not every message was delivered (remaining: " + numOfReceivedMEssages + ")");
		}
		
	}
	
	
	public static final class Message implements Serializable{
		public Long senderId;
		
		public Message() {
			senderId = -1L;
		}
		public Message(Long a) {
			this.senderId = a;
		}
	}

	public static final class CCUpdater extends VertexUpdateFunction<Long, Long, MessageWithSender<Long, Message>> {
		@Override
		public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator<MessageWithSender<Long, Message>> inMessages) {
			for (MessageWithSender<Long, Message> msg: inMessages) {
				System.out.println("Message from " + msg.sender + " to " + vertexKey);
//				if (! inNeighbours.get(vertexKey.intValue()).contains(msg.sender)) {
//					throw new RuntimeException("invalid message from " + msg + " to " + vertexKey);
//				} else {
//					numOfReceivedMEssages--;
//				}
				Tuple2<Long, Long> edge = new Tuple2<Long, Long>(msg.sender, vertexKey);
				if (!messageReceivedAlready.containsKey(edge)) {
					throw new RuntimeException("invalid message from " + msg.sender + " to " + vertexKey);
				} else {
					if (messageReceivedAlready.get(edge)) {
						throw new RuntimeException("Message from " + msg.sender
								+ " to " + vertexKey + " sent more than once.");
					} else {
						messageReceivedAlready.put(edge, true);
						numOfReceivedMEssages--;
					}
				}
			}
		}
	}
	
	public static final class CCMessager extends MessagingFunction2<Long, Long, Message, NullValue> {
		Message m = new Message(-1L);
		@Override
		public void sendMessages(Long vertexId, Long componentId) {
			m.senderId = vertexId;
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
