/**
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


import java.io.Serializable;
import java.util.Collection;

import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.spargel.java.VertexCentricIteration3;
import org.apache.flink.spargel.java.multicast.MCEnum;

/**
 * An implementation of the basic PageRank algorithm in the vertex-centric API (spargel).
 */
@SuppressWarnings("serial")
public class SpargelPageRankComputer implements Serializable {
	
	private final double BETA;
	private double epsilonForConvergence;
	private int whichMulticast = -1; 

	public SpargelPageRankComputer(double dumpeningFactor, 
			double epsilonForConvergence, int whichMulticast) {
		this.BETA = dumpeningFactor;
		this.epsilonForConvergence = epsilonForConvergence;
		this.whichMulticast = whichMulticast;
	}

	public DataSet<Tuple2<Long, SpargelNode>> computeNodeData(DataSet<Tuple1<Long>> numOfPages,
			DataSet<Tuple2<Long, long[]>> outNeighbourList,
			DataSet<Tuple2<Long, long[]>> inNeighbourList) {

		DataSet<Tuple2<Long, Double>> initialRanks = outNeighbourList
				.map(new RichMapFunction<Tuple2<Long, long[]>, Tuple2<Long, Double>>() {
					public long numOfVertices = -1l;

					public void open(Configuration parameters) throws Exception {
						Collection<Tuple1<Long>> broadcastSet = getRuntimeContext()
								.getBroadcastVariable(PageRankUtil.NUMOFPAGES);
						this.numOfVertices = broadcastSet.iterator().next().f0;
					}
					
					public Tuple2<Long, Double> map(Tuple2<Long, long[]> value) {
						return new Tuple2<Long, Double>(value.f0,
								1.0 / numOfVertices);
					}
				})
				.withBroadcastSet(numOfPages, PageRankUtil.NUMOFPAGES)
				.name("Assign initial pageranks");


		// We count the out edges from each node
		DataSet<Tuple2<Long, Long>> countOutEdges = outNeighbourList
				.map(new MapFunction<Tuple2<Long, long[]>, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(Tuple2<Long, long[]> value)
							throws Exception {
						return new Tuple2<Long, Long>(value.f0,  
								(Long) ((long)value.f1.length));
					}
				})
				.name("Number of outneighbours of the nodes");


		// identify sources
		DataSet<Tuple2<Long, Boolean>> isSource = inNeighbourList
				.map(new MapFunction<Tuple2<Long, long[]>, Tuple2<Long, Boolean>>() {
					@Override
					public Tuple2<Long, Boolean> map(Tuple2<Long, long[]> value)
							throws Exception {
						return new Tuple2<Long, Boolean>(value.f0,  
								value.f1.length == 0);
					}
				})
				.name("Identify sources");


		DataSet<Tuple2<Long, SpargelNode>> nodeData = initialRanks
				.join(countOutEdges)
				.where(0)
				.equalTo(0)
				// select and reorder fields of matching tuples
				.projectFirst(0, 1)
				.projectSecond(1)
				.types(Long.class, Double.class, Long.class)
				// source info
				.join(isSource)
				.where(0)
				.equalTo(0)
				.projectFirst(0, 1, 2)
				.projectSecond(1)
				.types(Long.class, Double.class, Long.class, Boolean.class)
				// compose the node data into a Node
				.map(new MapFunction<Tuple4<Long, Double, Long, Boolean>, Tuple2<Long, SpargelNode>>() {
					@Override
					public Tuple2<Long, SpargelNode> map(
							Tuple4<Long, Double, Long, Boolean> n)
							throws Exception {
						return new Tuple2<Long, SpargelNode>(n.f0, new SpargelNode(n.f0,
								n.f1, n.f2, n.f3));
					}
				})
				.name("Node data.");

		return nodeData;
	}




	public DataSet<Tuple2<Long, Double>> computePageRank(
			DataSet<Long> nodes,
			DataSet<Tuple2<Long, Long>> edges,
			DataSet<Tuple2<Long, long[]>> outNeighbourList, 
			int maxNumberOfIterations) {
		
		DataSet<Tuple2<Long, long[]>> inNeighbourList = PageRankUtil.createNeighbourList(nodes, edges, true, true);

		DataSet<Tuple1<Long>> numOfPages = PageRankUtil.getNumOfPages(nodes);
		
		DataSet<Tuple2<Long, SpargelNode>> nodeData = computeNodeData(numOfPages, outNeighbourList, inNeighbourList);
		
		CustomUnaryOperation<Tuple2<Long, SpargelNode>, Tuple2<Long, SpargelNode>> iteration = null;
		DoubleSumAggregator agg = new DoubleSumAggregator();
		DoubleMaxAggregator maxRankChange = new DoubleMaxAggregator();
		if (whichMulticast == 0) {
			// We do twice as many iterations because every second iteration deals with the sources only
			iteration = 
					VertexCentricIteration3.withPlainEdges(edges,
					new VertexRankUpdater(BETA),
					new RankMessenger(epsilonForConvergence, MCEnum.MC0),  2 * maxNumberOfIterations);
			VertexCentricIteration3<Long, SpargelNode, Double, ?> iteration2 = 
					(VertexCentricIteration3<Long, SpargelNode, Double, ?>)iteration;
			iteration2.addBroadcastSetForUpdateFunction(PageRankUtil.NUMOFPAGES, numOfPages);
			iteration2.registerAggregator(PageRankUtil.VALUE_COLLECTED_BY_SINKS, agg);
			iteration2.registerAggregator(PageRankUtil.MAX_RANK_CHANGE, maxRankChange);

		} else if (whichMulticast == 1) {
			// We do twice as many iterations because every second iteration deals with the sources only
			iteration = 
					VertexCentricIteration3.withPlainEdges(edges,
					new VertexRankUpdater(BETA),
					new RankMessenger1(epsilonForConvergence, MCEnum.MC1),  2 * maxNumberOfIterations);
			VertexCentricIteration3<Long, SpargelNode, Double, ?> iteration2 = 
					(VertexCentricIteration3<Long, SpargelNode, Double, ?>)iteration;
			iteration2.addBroadcastSetForUpdateFunction(PageRankUtil.NUMOFPAGES, numOfPages);
			iteration2.registerAggregator(PageRankUtil.VALUE_COLLECTED_BY_SINKS, agg);
			iteration2.registerAggregator(PageRankUtil.MAX_RANK_CHANGE, maxRankChange);
		} else if (whichMulticast == 2) {
			// We do twice as many iterations because every second iteration deals with the sources only
			iteration = 
					VertexCentricIteration3.withPlainEdges(edges,
					new VertexRankUpdater(BETA),
					new RankMessenger2(epsilonForConvergence, MCEnum.MC2),  2 * maxNumberOfIterations);
			VertexCentricIteration3<Long, SpargelNode, Double, ?> iteration2 = 
					(VertexCentricIteration3<Long, SpargelNode, Double, ?>)iteration;
			iteration2.addBroadcastSetForUpdateFunction(PageRankUtil.NUMOFPAGES, numOfPages);
			iteration2.registerAggregator(PageRankUtil.VALUE_COLLECTED_BY_SINKS, agg);
			iteration2.registerAggregator(PageRankUtil.MAX_RANK_CHANGE, maxRankChange);
		} else {
			throw new RuntimeException(
					"The value of <whichMulticast>  should be 0, 1, or 2");
		}
		
		DataSet<Tuple2<Long, SpargelNode>> nodeRanks = nodeData.runOperation(iteration);

		DataSet<Tuple2<Long, Double>> result = nodeRanks.map(
				new MapFunction<Tuple2<Long,SpargelNode>, Tuple2<Long,Double>>() {

					@Override
					public Tuple2<Long, Double> map(
							Tuple2<Long, SpargelNode> value) throws Exception {
						return new Tuple2<Long, Double>(value.f0, value.f1.getRank());
					}
		});
		
		return result;
	}
//	public static void main(String[] args) throws Exception {
//		
//		new SpargelPageRankComputer().run();
//	}
	
	
}
