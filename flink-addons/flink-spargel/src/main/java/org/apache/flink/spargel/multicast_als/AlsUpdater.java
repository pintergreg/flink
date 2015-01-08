package org.apache.flink.spargel.multicast_als;

import java.util.Map;

import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.spargel.multicast_test.io_utils.LinalgSolver;

public class AlsUpdater
		extends
		VertexUpdateFunction<Integer, DoubleVectorWithMap, AlsCustomMessageForSpargel> {

	private int k;
	private double lambda;
	private String whichSolver;
	private LinalgSolver solver = new LinalgSolver();

	public AlsUpdater(int k, double lambda, String whichSolver) {
		this.k = k;
		this.lambda = lambda;
		this.whichSolver = whichSolver;
	}

	@Override
	public void updateVertex(Integer vertexKey,
			DoubleVectorWithMap vertexValue,
			MessageIterator<AlsCustomMessageForSpargel> inMessages)
			throws Exception {
		int modSuperstep = getSuperstepNumber() % 2;
		DoubleVectorWithMap updatedValue = new DoubleVectorWithMap();

		if (vertexKey % 2 == modSuperstep) {
			double[][] matrix = new double[k][k];
			double[][] vector = new double[k][1];

			// Regularization
			if (lambda != 0.0) {
				for (int i = 0; i < k; i++) {
					matrix[i][i] = lambda;
				}
			}

			// Don't do anything if q is empty
			if (!inMessages.hasNext()) {
				return;
			}

			Map<String, Double> ratings = vertexValue.getEdges();
			if (ratings == null) {
				throw new NullPointerException("In the " + getSuperstepNumber()
						+ ". iteration: The edgemap cannot be null!");
			}

			for (AlsCustomMessageForSpargel column : inMessages) {
				if (column.getId() % 2 == modSuperstep) {
					throw new RuntimeException(
							"In the "
									+ getSuperstepNumber()
									+ ". iteration: The incoming message has incorrect vertexId mod 2!");
				}

				double[] column_elements = column.getData();

				if (column_elements.length != k) {
					throw new IllegalArgumentException("In the "
							+ getSuperstepNumber()
							+ ". iteration: The size of k=" + k + " and"
							+ " the message data.length="
							+ column_elements.length + " are not the same!");
				}

				double rating = ratings.get(Integer.toString(column.getId()));
				for (int i = 0; i < k; ++i) {
					for (int j = 0; j < k; ++j) {
						matrix[i][j] += column_elements[i] * column_elements[j];
					}
					vector[i][0] += rating * column_elements[i];
				}
			}

			double[] result_elements = solver.solve(whichSolver, k, matrix,
					vector);

			updatedValue.setId(vertexValue.getId());
			updatedValue.setData(result_elements);
			updatedValue.setEdges(vertexValue.getEdges());

			setNewVertexValue(updatedValue);

		}
	}

}
