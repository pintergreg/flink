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
package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.spargel.java.VertexCentricIteration3;
import org.apache.flink.spargel.java.multicast.MCEnum;

//TODO: usage of union operator fails

public class SpargelAls {

	public void runAls(int noSubTasks, String matrixInput, String output,
			int k, double lambda, int iteration,
			String whichSolver, int whichMulticast) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// input rating matrix
		DataSet<Tuple3<Integer, Integer, Double>> matrixSource = env
				.readCsvFile(matrixInput).fieldDelimiter('|')
				.lineDelimiter("|\n")
				.types(Integer.class, Integer.class, Double.class);

		// create the q_with_data dataset
		DataSet<Tuple2<Integer, DoubleVectorWithMap>> q_with_data = matrixSource
				.groupBy(1).reduceGroup(new RandomMatrix(k))
				.name("Create q as a random matrix");

		// create the p_with_data dataset
		DataSet<Tuple2<Integer, DoubleVectorWithMap>> p_with_data = matrixSource
				.groupBy(0).reduceGroup(new PFormatter())
				.name("Format p dataset");

		// find the edgemaps of q vertices
		DataSet<Tuple2<Integer, DoubleVectorWithMap>> q_with_edges = matrixSource
				.groupBy(1).reduceGroup(new FindEdgeMap(1))
				.name("Find the edgesmaps of p");

		// find the edgemaps of p vertices
		DataSet<Tuple2<Integer, DoubleVectorWithMap>> p_with_edges = matrixSource
				.groupBy(0).reduceGroup(new FindEdgeMap(0))
				.name("Find the edgesmaps of p");

		// create the vertices of the graph
		DataSet<Tuple2<Integer, DoubleVectorWithMap>> q_vertices = q_with_data
				.join(q_with_edges).where(0).equalTo(0)
				.with(new CreateVertices())
				.name("Create the q vertices of the graph");

		DataSet<Tuple2<Integer, DoubleVectorWithMap>> p_vertices = p_with_data
				.join(p_with_edges).where(0).equalTo(0)
				.with(new CreateVertices())
				.name("Create the p vertices of the graph");

		DataSet<Tuple2<Integer, DoubleVectorWithMap>> vertices = p_vertices
				.coGroup(q_vertices).where(0).equalTo(0)
				.with(new UnionVertices())
				.name("Union the p and q vertice groups");

		// create the edges of the graph
		DataSet<Tuple2<Integer, Integer>> edges = matrixSource.flatMap(
				new CreateEdges()).name("Create the edges of the graph");

		DataSet<Tuple2<Integer, DoubleVectorWithMap>> result = null;
		if (whichMulticast == 0) {
			VertexCentricIteration3<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, ?> vc_iteration = VertexCentricIteration3
					.withPlainEdges(edges, new AlsUpdater(k, lambda,
							whichSolver), new AlsMessager(MCEnum.MC0), 2 * iteration + 1);
			// Stephan's workaround: is it needed for big input?
			vc_iteration.setSolutionSetUnmanagedMemory(true);

			result = vertices.runOperation(vc_iteration);
		} else if (whichMulticast == 1) {
			VertexCentricIteration3<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, ?> vc_iteration1 = VertexCentricIteration3
					.withPlainEdges(edges, new AlsUpdater(k, lambda,
							whichSolver), new AlsMessager1(MCEnum.MC1), 2 * iteration + 1);
			// Stephan's workaround: is it needed for big input?
			vc_iteration1.setSolutionSetUnmanagedMemory(true);
			
			result = vertices.runOperation(vc_iteration1);
		} else if (whichMulticast == 2) {
			// must I use long id?
			VertexCentricIteration3<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel, ?> vc_iteration2 = VertexCentricIteration3
					.withPlainEdges(edges, new AlsUpdater(k, lambda,
							whichSolver), new AlsMessager2(MCEnum.MC2), 2 * iteration + 1);
			// Stephan's workaround: is it needed?
			vc_iteration2.setSolutionSetUnmanagedMemory(true);
			
			result = vertices.runOperation(vc_iteration2);
		} else {
			throw new RuntimeException(
					"The value of <whichMulticast>  should be 0, 1, or 2");
		}

		// delete marker fields
		DataSet<Tuple2<Integer, double[]>> pOutFormat = result.groupBy(0)
				.reduceGroup(new OutputFormatter(false))
				.name("P output format");

		DataSet<Tuple2<Integer, double[]>> qOutFormat = result.groupBy(0)
				.reduceGroup(new OutputFormatter(true)).name("Q output format");

		// output
		ColumnOutputFormat pFormat = new ColumnOutputFormat(output + "/p");
		pFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<Tuple2<Integer, double[]>> pSink = pOutFormat.output(pFormat);

		ColumnOutputFormat qFormat = new ColumnOutputFormat(output + "/q");
		qFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<Tuple2<Integer, double[]>> qSink = qOutFormat.output(qFormat);

		env.setDegreeOfParallelism(noSubTasks);

		env.execute(getClass().getSimpleName() + "_" + whichMulticast + "MC");
	}

	public static void main(String[] args) throws Exception {

		// parse job parameters
		int numTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String input = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");
		int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
		double lambda = (args.length > 4 ? Double.parseDouble(args[4]) : 0.0);
		int numIterations = (args.length > 5 ? Integer.parseInt(args[5]) : 1);

		new SpargelAls().runAls(numTasks, input, output, k, lambda,
				numIterations, "jama", 0);
	}
}
