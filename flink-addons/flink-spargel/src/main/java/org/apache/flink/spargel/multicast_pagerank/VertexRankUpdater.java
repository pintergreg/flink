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
package org.apache.flink.spargel.multicast_pagerank;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.types.DoubleValue;

/**
 * Function that updates the rank of a vertex by summing up the partial ranks from all incoming messages
 * and then applying the dampening formula.
 */
public class VertexRankUpdater extends VertexUpdateFunction<Long, SpargelNode, Double> {
	
	private static final long serialVersionUID = 1L;
	private final double beta;
	private long numOfVertices;
	
	// Dealing with the value collected by the sinks
	private DoubleValue sentBySinks;
	double contributionBySinks;

	// This is needed for the epsilon filter
	//private Map<Long, Double> previousNodeRanks = new HashMap<Long, Double>();
	private DoubleMaxAggregator maxChange;

	
	@Override
	public void preSuperstep() {
		//System.out.println("SuperStep: " + getSuperstepNumber());
		numOfVertices = this.<Tuple1<Long>>getBroadcastSet(PageRankUtil.NUMOFPAGES).iterator().next().f0;
		if (getSuperstepNumber() % 2 == 0) {
			sentBySinks =  getPreviousIterationAggregate(PageRankUtil.VALUE_COLLECTED_BY_SINKS);
			contributionBySinks = beta * sentBySinks.getValue() / numOfVertices;
			maxChange = getIterationAggregator(PageRankUtil.MAX_RANK_CHANGE);
		}
		
	}

	public VertexRankUpdater(double beta) {
		this.beta = beta;
	}

	@Override
	public void updateVertex(Long vertexKey, SpargelNode vertexValue, MessageIterator<Double> inMessages) {
		if (getSuperstepNumber() % 2 == 1) {

			
			// Let's remember the current rank for a sec until the end of the next iteration
			vertexValue.setPreviousRank(vertexValue.getRank());

			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}

			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1 - beta) / numOfVertices;
			vertexValue.setRank(newRank);
			setNewVertexValue(vertexValue);
		} else {
			// in every second superstep we deal with the values sent by the
			// sinks
			double rank = vertexValue.getRank();
			rank += contributionBySinks;
			vertexValue.setRank(rank);
			maxChange.aggregate(Math.abs(vertexValue.getPreviousRank() - rank)); 
			setNewVertexValue(vertexValue);
		}

	}
}
