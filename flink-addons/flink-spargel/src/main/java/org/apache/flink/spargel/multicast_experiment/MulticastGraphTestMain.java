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


import java.io.Serializable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.spargel.multicast_test.io_utils.EdgeListInputFormat;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;


public class MulticastGraphTestMain implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String NUM_OF_RECEIVED_MESSAGES = "NUM_OF_RECEIVED_MESSAGES";

	private int whichMulticast = 0;
	private String inputFile = null;
	private int degreeOfParalellism = 0;
	private int numberOfIterations = 0;
	private transient DataSet<Tuple2<Long, Long>> edges;// = env.fromCollection(edgeList);
	private transient DataSet<Tuple2<Long, VertexVal>> initialVertices;
	
	@SuppressWarnings("unchecked")
	private  void loadInput(ExecutionEnvironment env) {
		// Read the input
		if (inputFile == null) {

			Long numOfVertices = 11L;


			initialVertices = env.generateSequence(0L, numOfVertices - 1).map(new IdAssigner());
			// set of edges
			edges = env.fromElements(new Tuple2<Long, Long>(0L, 2L),
					new Tuple2<Long, Long>(2L, 4L), new Tuple2<Long, Long>(0L,
							5L), new Tuple2<Long, Long>(4L, 8L),
					new Tuple2<Long, Long>(1L, 5L), new Tuple2<Long, Long>(3L,
							7L),
					new Tuple2<Long, Long>(3L, 9L),
					new Tuple2<Long, Long>(7L, 9L),
					new Tuple2<Long, Long>(2L, 9L),
					// we make sure no sinks/sources remain
					new Tuple2<Long, Long>(0L, 1L), new Tuple2<Long, Long>(1L,
							2L), new Tuple2<Long, Long>(2L, 3L),
					new Tuple2<Long, Long>(3L, 4L), new Tuple2<Long, Long>(4L,
							5L), new Tuple2<Long, Long>(5L, 6L),
					new Tuple2<Long, Long>(6L, 7L), new Tuple2<Long, Long>(7L,
							8L), new Tuple2<Long, Long>(8L, 9L),
					new Tuple2<Long, Long>(9L, 1L),
					// new Tuple2<Long, Long>(9L, 10L),
					new Tuple2<Long, Long>(10L, 0L));

		} else {

			// create input dataset
			DataSet<Tuple2<Long, long[]>> outNeighbourList = env.createInput(new EdgeListInputFormat(
					inputFile));

			//outNeighbourList.print();
			
			initialVertices = outNeighbourList
					.map(new MapFunction<Tuple2<Long, long[]>, Long>() {
						@Override
						public Long map(Tuple2<Long, long[]> value)
								throws Exception {
							return value.f0;
						}
					}).map(new IdAssigner());;

			edges = outNeighbourList
					.flatMap(new FlatMapFunction<Tuple2<Long, long[]>, Tuple2<Long, Long>>() {

						@Override
						public void flatMap(Tuple2<Long, long[]> value,
								Collector<Tuple2<Long, Long>> out)
										throws Exception {
							for (Long d : value.f1) {
								out.collect(new Tuple2<Long, Long>(value.f0, d));
							}
						}
					});
		}
	}



	private void run(String[] args) throws Exception {
		if (args.length == 4) {
			
			// some input data
			whichMulticast = Integer.parseInt(args[0]);
			inputFile = args[1];
			degreeOfParalellism = Integer.parseInt(args[2]);
			numberOfIterations = Integer.parseInt(args[3]);
		} else  if (args.length == 0) {
			// default
			System.out.println(" Running spargel multicast on a graph with default parameters");
			whichMulticast = 2;
			degreeOfParalellism = 4;
			numberOfIterations = 10;

		} else {
			System.err.println("Usage: <whichMulticast> <inputFile> <degreeOfParalellism> <numberOfIterations>" );
			System.err.println("<whichMulticast> is either 0, 1 or 2." );
			System.exit(1);
		}


		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		loadInput(env);

		System.out.println("Parameters:" );
		System.out.println("<whichMulticast>: " + whichMulticast);
		System.out.println("<inputFile>: " + inputFile);
		System.out.println("<degreeOfParalellism>: " + degreeOfParalellism);
		System.out.println("<numberOfIterations>: "  + numberOfIterations);
		
		//DataSet<Long> vertexIds = env.generateSequence(0, numOfNodes - 1);


		DataSet<Tuple2<Long, VertexVal>> result = null;

		if (whichMulticast == 0) {
			VertexCentricIteration<Long, VertexVal, Message, ?> iteration = VertexCentricIteration
					.withPlainEdges(edges, new CCUpdater(), new CCMessager(), numberOfIterations);
			result = initialVertices.runOperation(iteration);
		} else if (whichMulticast == 1) {
			VertexCentricIteration1<Long, VertexVal, Message, ?> iteration = VertexCentricIteration1
					.withPlainEdges(edges, new CCUpdater(), new CCMessager1(),
							numberOfIterations);
			result = initialVertices.runOperation(iteration);
		} else if (whichMulticast == 2) {
			VertexCentricIteration2<Long, VertexVal, Message, ?> iteration = VertexCentricIteration2
					.withPlainEdges(edges, new CCUpdater(), new CCMessager2(),
							numberOfIterations);
			result = initialVertices.runOperation(iteration);
		} else {
			throw new RuntimeException(
					"The value of <whichMulticast>  should be 0, 1, or 2");
		}

		result.sum(0).print();
		env.setDegreeOfParallelism(degreeOfParalellism);
		
		
//		System.out.println("Get execution plan.");
//		System.out.println(env.getExecutionPlan());
//		System.exit(1);
		
		JobExecutionResult jobRes = env
				.execute("Spargel Multiple recipients test with multicast "
						+ whichMulticast);
		
		Long numOfRecMsgs = jobRes
				.getAccumulatorResult(NUM_OF_RECEIVED_MESSAGES);
		System.out.println("Net runtime: " + jobRes.getNetRuntime());
		System.out.println("Number of recieved messages: " + numOfRecMsgs);
		
//We could check here whether the number of messages was correct
		//		long expectedNumOfMessages = (long)edgeList.size() * (long)numberOfIterations;
//		if (numOfRecMsgs != expectedNumOfMessages) {
//			throw new RuntimeException("The number of received messages (per iteration) was "
//					+ numOfRecMsgs + " instead of " + expectedNumOfMessages);
//		}

	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Testing spargel multicast on a graph." );

		MulticastGraphTestMain instance = new MulticastGraphTestMain();
		instance.run(args);

		
	}

	public static final class VertexVal {
		public Integer dummy = 1;
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
		

		@Override
		public void preSuperstep() throws Exception {
			//System.out.println("SuperStep: " + getSuperstepNumber());
			getRuntimeContext().addAccumulator(MulticastGraphTestMain.NUM_OF_RECEIVED_MESSAGES, new LongCounter());
		}
		
		@Override
		public void updateVertex(Long vertexKey, VertexVal vertexValue, MessageIterator<Message> inMessages) {
			long msgcount = 0;
			for (Message msg: inMessages) {
				//System.out.println("Message from " + msg.senderId + " to " + vertexKey);
				msgcount ++;
			}
			getRuntimeContext().getAccumulator(MulticastGraphTestMain.NUM_OF_RECEIVED_MESSAGES).add(msgcount);
			//numOfRecievedMessages.add(msgcount);
			setNewVertexValue(vertexValue);
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
