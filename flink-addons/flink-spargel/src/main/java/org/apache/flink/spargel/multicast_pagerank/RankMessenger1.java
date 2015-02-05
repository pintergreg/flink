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


import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.multicast.MCEnum;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.NullValue;

/**
 * Distributes the rank of a vertex among all target vertices according to the transition probability,
 * which is associated with an edge as the edge value.
 */
public class RankMessenger1 extends MessagingFunction<Long, SpargelNode, Double, NullValue> {

	private static final long serialVersionUID = 1L;
	private DoubleSumAggregator agg;

	// This is needed for the epsilon filter
	private DoubleValue maxChangeValueInPrevIter;
	private double epsilonForConvergence; 

	public RankMessenger1(double epsilonForConvergence, MCEnum whichMulticast) {
		super(whichMulticast);
		this.epsilonForConvergence = epsilonForConvergence;
	}
	
	@Override
	public void preSuperstep() {
		agg = getIterationAggregator(PageRankUtil.VALUE_COLLECTED_BY_SINKS);
		//agg.reset();
		if ((getSuperstepNumber() > 1) && (getSuperstepNumber() % 2 == 1)) {
			maxChangeValueInPrevIter = getPreviousIterationAggregate(PageRankUtil.MAX_RANK_CHANGE);
			//System.out.println("Max change in previous 2 iterations: " + maxChangeValueInPrevIter.getValue());
		}

	}

	@Override
	public void sendMessages(Long vertexId, SpargelNode vertexValue) {
		if (getSuperstepNumber() % 2 == 1) {
			if (getSuperstepNumber() > 1) {
				//We check the convergence criterion
				if (maxChangeValueInPrevIter.getValue() < epsilonForConvergence){
					// no vertex updates will be done
					return;
				}
			}
			sendMessageToAllNeighbors(vertexValue.getRank()	/ vertexValue.getOutDegree());
			if (vertexValue.isSource) {
				sendMessageTo(vertexId, 0.0);
			}
			if (vertexValue.getOutDegree() == 0) {
				// deal with the sinks
				// agg = getIterationAggregator("valueCollectedBySinks");
				// System.out.println("Superstep: " + getSuperstepNumber() +
				// ", "
				// + n.getCurrentPageRank());
				agg.aggregate(vertexValue.getRank());
			}

		} else {
			// in every second round we send a dummy message to everyone
			// to wake them up and update the value sent by the sinks
			sendMessageTo(vertexId, 0.0);
		}

	}
}
