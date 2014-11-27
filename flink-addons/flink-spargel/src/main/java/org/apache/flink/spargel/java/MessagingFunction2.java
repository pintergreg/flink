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

package org.apache.flink.spargel.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.spargel.java.multicast.MessageWithSender;
import org.apache.flink.spargel.java.multicast.MultipleRecipients;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

/**
 * The base class for functions that produce messages between vertices as a part of a {@link VertexCentricIteration}.
 * 
 * @param <VertexKey> The type of the vertex key (the vertex identifier).
 * @param <VertexValue> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EdgeValue> The type of the values that are associated with the edges.
 */
public abstract class MessagingFunction2<VertexKey extends Comparable<VertexKey>, VertexValue, 
	Message , EdgeValue> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This method is invoked once per superstep for each vertex that was changed in that superstep.
	 * It needs to produce the messages that will be received by vertices in the next superstep.
	 * 
	 * @param vertexKey The key of the vertex that was changed.
	 * @param vertexValue The value (state) of the vertex that was changed.
	 * 
	 * @throws Exception The computation may throw exceptions, which causes the superstep to fail.
	 */
	public abstract void sendMessages(VertexKey vertexKey, VertexValue vertexValue) throws Exception;
	
	/**
	 * This method is executed one per superstep before the vertex update function is invoked for each vertex.
	 * 
	 * @throws Exception Exceptions in the pre-superstep phase cause the superstep to fail.
	 */
	public void preSuperstep() throws Exception {}
	
	/**
	 * This method is executed one per superstep after the vertex update function has been invoked for each vertex.
	 * 
	 * @throws Exception Exceptions in the post-superstep phase cause the superstep to fail.
	 */
	public void postSuperstep() throws Exception {}
	
	
	/**
	 * Gets an {@link java.lang.Iterable} with all outgoing edges. This method is mutually exclusive with
	 * {@link #sendMessageToAllNeighbors(Object)} and may be called only once.
	 * 
	 * @return An iterator with all outgoing edges.
	 */
	@SuppressWarnings("unchecked")
	public Iterable<OutgoingEdge<VertexKey, EdgeValue>> getOutgoingEdges() {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getOutgoingEdges()' or 'sendMessageToAllTargets()' exactly once.");
		}
		edgesUsed = true;

		this.edgeNoValueIter.set((Iterator<Tuple2<VertexKey, VertexKey>>) edges);
		return this.edgeNoValueIter;
//		
//		if (this.edgeWithValueIter != null) {
//			this.edgeWithValueIter.set((Iterator<Tuple3<VertexKey, VertexKey, EdgeValue>>) edges);
//			return this.edgeWithValueIter;
//		} else {
//			this.edgeNoValueIter.set((Iterator<Tuple2<VertexKey, VertexKey>>) edges);
//			return this.edgeNoValueIter;
//		}
	}
	
//	private MessageWithSender<VertexKey, Message> mWS = new MessageWithSender<VertexKey, Message>();
//	private Tuple2<VertexKey, MessageWithSender<VertexKey, Message>> outValue2;
//	private Collector<Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>> out2;
	
//	public void sendMessageToAllNeighbors2(VertexKey sender, Message m) {
//		if (edgesUsed) {
//			throw new IllegalStateException("Can use either 'getOutgoingEdges()' or 'sendMessageToAllTargets()' exactly once.");
//		}
//		
//		edgesUsed = true;
//		mWS.sender = sender;
//		mWS.message = m;
//				
//		outValue2.f1 = mWS;
//		channelSet.clear();
//	
//		while (edges.hasNext()) {
//			Tuple next = (Tuple) edges.next();
//			VertexKey k = next.getField(1);
//			outValue2.f0 = k;
//			channel = ((OutputCollector<Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>>)out2).getChannel(outValue2);
//			if (!channelSet.contains(channel)){
//				channelSet.add(channel);
//				out2.collect(outValue2);
//			}
//		}
//	}

//	private Set<Integer> channelSet = new HashSet<Integer>();
	private Integer channel;
	
	private Map<Integer, List<VertexKey>> recipientsInBlock = new HashMap <Integer, List<VertexKey>>();

	
	private Collector<Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>> out;
	private Tuple2<VertexKey, MessageWithSender<VertexKey, Message>> outValue;// = new Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>();
	//private MessageWithSender<VertexKey, Message> msgWithSender = new MessageWithSender<VertexKey, Message>();
	//private MessageWithSender<VertexKey, Message> msgWithSender = new MessageWithSender<VertexKey, Message>();
	
	public void setSender(VertexKey sender) {
		outValue.f1.sender = sender;
		
	}
	
	@SuppressWarnings("unchecked")
	public void sendMessageToMultipleRecipients(MultipleRecipients<VertexKey> recipients, Message m) {
		//Clear previous contents
//		channelSet.clear();
//		for (List<VertexKey> targets: recipientsInBlock.values()) {
//			targets.clear();
//		}
		//		blockedRecipients.clear();
		recipientsInBlock.clear();
		//System.out.println(outValue.f1.getSender());
		outValue.f1.message = m;
		//outValue.f1.sender is already set
		for (VertexKey target: recipients) {
			outValue.f0 = target;
			channel = ((OutputCollector<Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>>)out).getChannel(outValue);
			if (recipientsInBlock.get(channel) == null) {
				recipientsInBlock.put(channel, new ArrayList<VertexKey>());
//			if (!channelSet.contains(channel)){
//				channelSet.add(channel);
//				//out.collect(outValue);
//				if (recipientsInBlock.get(channel) == null) {
//					recipientsInBlock.put(channel, new ArrayList<VertexKey>());
//				}
//				recipientsInBlock.get(channel).clear();
			}
			recipientsInBlock.get(channel).add(target);
		}
		for (List<VertexKey> targets: recipientsInBlock.values()) {
			System.out.println("Sender: " + outValue.f1.sender);
			System.out.println("Targets: " + targets);
			outValue.f0 = targets.get(0);
			outValue.f1.someRecipients = (VertexKey[])targets.toArray(new Comparable[0]); 
			out.collect(outValue);
			System.out.println(outValue);
		}
	}

	private MultipleRecipients<VertexKey> recipients = new MultipleRecipients<VertexKey>();
	/**
	 * Sends the given message to all vertices that are targets of an outgoing edge of the changed vertex.
	 * This method is mutually exclusive to the method {@link #getOutgoingEdges()} and may be called only once.
	 * 
	 * @param m The message to send.
	 */
	public void sendMessageToAllNeighbors(Message m) {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getOutgoingEdges()' or 'sendMessageToAllTargets()' exactly once.");
		}
		
		edgesUsed = true;
		recipients.clear();
		while (edges.hasNext()) {
			Tuple next = (Tuple) edges.next();
			VertexKey k = next.getField(1);
			recipients.addRecipient(k);
//
//			outValue.f0 = k;
//			out.collect(outValue);
		}
		sendMessageToMultipleRecipients(recipients, m);
	}

	
	
	/**
	 * Sends the given message to the vertex identified by the given key. If the target vertex does not exist,
	 * the next superstep will cause an exception due to a non-deliverable message.
	 * 
	 * @param target The key (id) of the target vertex to message.
	 * @param m The message.
	 */

	public void sendMessageTo(VertexKey target, Message m) {
		recipients.clear();
		recipients.addRecipient(target);
		sendMessageToMultipleRecipients(recipients, m);
	}


	
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of the superstep, starting at <tt>1</tt>.
	 * 
	 * @return The number of the current superstep.
	 */
	public int getSuperstepNumber() {
		return this.runtimeContext.getSuperstepNumber();
	}
	
	/**
	 * Gets the iteration aggregator registered under the given name. The iteration aggregator is combines
	 * all aggregates globally once per superstep and makes them available in the next superstep.
	 * 
	 * @param name The name of the aggregator.
	 * @return The aggregator registered under this name, or null, if no aggregator was registered.
	 */
	public <T extends Aggregator<?>> T getIterationAggregator(String name) {
		return this.runtimeContext.<T>getIterationAggregator(name);
	}
	
	/**
	 * Get the aggregated value that an aggregator computed in the previous iteration.
	 * 
	 * @param name The name of the aggregator.
	 * @return The aggregated value of the previous iteration.
	 */
	public <T extends Value> T getPreviousIterationAggregate(String name) {
		return this.runtimeContext.<T>getPreviousIterationAggregate(name);
	}
	
	/**
	 * Gets the broadcast data set registered under the given name. Broadcast data sets
	 * are available on all parallel instances of a function. They can be registered via
	 * {@link VertexCentricIteration#addBroadcastSetForMessagingFunction(String, org.apache.flink.api.java.DataSet)}.
	 * 
	 * @param name The name under which the broadcast set is registered.
	 * @return The broadcast data set.
	 */
	public <T> Collection<T> getBroadcastSet(String name) {
		return this.runtimeContext.<T>getBroadcastVariable(name);
	}

	// --------------------------------------------------------------------------------------------
	//  internal methods and state
	// --------------------------------------------------------------------------------------------
	
	
	private IterationRuntimeContext runtimeContext;
	
	private Iterator<?> edges;
	
	
	private EdgesIteratorNoEdgeValue<VertexKey, EdgeValue> edgeNoValueIter;
	
	private boolean edgesUsed;
	
	
	void init(IterationRuntimeContext context, boolean hasEdgeValue) {
		this.runtimeContext = context;
		this.outValue = new Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>();
		//this.msgWithSender = new MessageWithSender<VertexKey, Message>();
		this.outValue.f1 = new MessageWithSender<VertexKey, Message>();
		
		if (hasEdgeValue) {
			throw new RuntimeException("Edge values not supported");
			//this.edgeWithValueIter = new EdgesIteratorWithEdgeValue<VertexKey, EdgeValue>();
		} else {
			this.edgeNoValueIter = new EdgesIteratorNoEdgeValue<VertexKey, EdgeValue>();
		}
	}
	
	void set(Iterator<?> edges, Collector<Tuple2<VertexKey, MessageWithSender<VertexKey, Message>>> out) {
		this.edges = edges;
		this.out = out;
		this.edgesUsed = false;
	}
	
	
	
	private static final class EdgesIteratorNoEdgeValue<VertexKey extends Comparable<VertexKey>, EdgeValue> 
		implements Iterator<OutgoingEdge<VertexKey, EdgeValue>>, Iterable<OutgoingEdge<VertexKey, EdgeValue>>
	{
		private Iterator<Tuple2<VertexKey, VertexKey>> input;
		
		private OutgoingEdge<VertexKey, EdgeValue> edge = new OutgoingEdge<VertexKey, EdgeValue>();
		
		
		void set(Iterator<Tuple2<VertexKey, VertexKey>> input) {
			this.input = input;
		}
		
		@Override
		public boolean hasNext() {
			return input.hasNext();
		}

		@Override
		public OutgoingEdge<VertexKey, EdgeValue> next() {
			Tuple2<VertexKey, VertexKey> next = input.next();
			edge.set(next.f1, null);
			return edge;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<OutgoingEdge<VertexKey, EdgeValue>> iterator() {
			return this;
		}
	}
	
	
}
