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



import java.io.Serializable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.spargel.multicast_test.io_utils.EdgeListInputFormat;
import org.apache.flink.util.Collector;


public class SpargelPageRankMain implements Serializable{

	private static final long serialVersionUID = 1L;

	public static final double DAMPENING_FACTOR = 0.85;

	private int maxNumberOfIterations;
	private int degreeOfParallelism;
	private boolean fileOutput = false;
	private String outputPath = null;
	private String edgeListInputPath = null;
	private int whichMulticast = -1; 
	
	private double epsilonForConvergence; 

	public SpargelPageRankMain(double epsilon) {
		super();
		this.epsilonForConvergence = epsilon;
	}

	// Input data
	// set of nodes
	private transient DataSet<Long> nodes;
	// set of edges
	private transient DataSet<Tuple2<Long, Long>>  edges;
	private transient DataSet<Tuple2<Long, long[]>> outNeighbourList;
	
	public void runPageRank() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		loadInput(env);
		System.out.println("Parameters:" );
		System.out.println("<whichMulticast>: " + whichMulticast);
		System.out.println("<degreeOfParalellism>: " + degreeOfParallelism);
		System.out.println("<maxNumberOfIterations>: "  + maxNumberOfIterations);
		System.out.println("<edgeListInputPath>: " + edgeListInputPath);
		System.out.println("<outputPath>: " + outputPath);
	
		DataSet<Tuple2<Long, Double>> nodeRanks = 
				new SpargelPageRankComputer(DAMPENING_FACTOR, epsilonForConvergence, whichMulticast)
		.computePageRank(nodes, edges, outNeighbourList, maxNumberOfIterations);

		if (fileOutput) {
			CsvOutputFormat<Tuple2<Long, Double>> rankOutputFormat = new CsvOutputFormat<Tuple2<Long, Double>>(
					new Path(outputPath + "/ranks"), "\n", " ");
			rankOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			// DataSink<Tuple2<Long, Double>> rankOutput =
			nodeRanks.output(rankOutputFormat);
			nodeRanks.project(1).types(Double.class).sum(0)
					.writeAsCsv(outputPath + "/PR_SUM", "|\n", "|");
		} else {
			nodeRanks.sum(1).print();
		}

		env.setDegreeOfParallelism(degreeOfParallelism);

		env.execute("Spargel PageRank");
		
	}

	@SuppressWarnings("unchecked")
	private  void loadInput(ExecutionEnvironment env) {
		// Read the input
		if (!fileOutput) {

			Long numOfVertices = 11L;
			//generateRandomGraph(env, numOfVertices);
			
			// Load default data
			maxNumberOfIterations = 20;
			// number of cores
			degreeOfParallelism = 1;
			
			whichMulticast = 0;
			// set of nodes
			
			nodes = env.generateSequence(0, numOfVertices - 1);

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

			outNeighbourList = PageRankUtil.createNeighbourList(nodes,
						edges, false, true);

		} else {

			// create input dataset
			outNeighbourList = env.createInput(new EdgeListInputFormat(
					edgeListInputPath));

			//outNeighbourList.print();
			
			nodes = outNeighbourList
					.map(new MapFunction<Tuple2<Long, long[]>, Long>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Long map(Tuple2<Long, long[]> value)
								throws Exception {
							return value.f0;
						}
					});

			edges = outNeighbourList
					.flatMap(new FlatMapFunction<Tuple2<Long, long[]>, Tuple2<Long, Long>>() {
						private static final long serialVersionUID = 1L;

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
//
	public void setFileIOParameters(String edgeListInputPath1,
			String outputPath1, 
			int maxNumberOfIterations1, 
			int degreeOfParallelism1) {
		fileOutput = true;
		edgeListInputPath = edgeListInputPath1;
		outputPath = outputPath1;
		maxNumberOfIterations = maxNumberOfIterations1;
		degreeOfParallelism = degreeOfParallelism1;
	}

	private boolean parseParameters(String[] args) {
		String usageMessage = this.getClass().getSimpleName() + " usage: <edgeListInputPath> <outputPath> <maxIterations> <degreeOfParalellism>";

		if(args.length > 0) {
			if(args.length == 4) {
				fileOutput = true;
				degreeOfParallelism = Integer.parseInt(args[0]);
				edgeListInputPath = args[1];
				outputPath = args[2];
				maxNumberOfIterations = Integer.parseInt(args[2]);
				whichMulticast = Integer.parseInt(args[3]);
			} else {
				System.err
				.println(usageMessage);
				return false;
			}
		} else {
			System.out.println("Executing SpargelPageRank example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out
			.println(usageMessage);
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		SpargelPageRankMain instance = new SpargelPageRankMain(0.00001);
		if(!instance.parseParameters(args)) {
			return;
		}
		instance.runPageRank();
	}


}
