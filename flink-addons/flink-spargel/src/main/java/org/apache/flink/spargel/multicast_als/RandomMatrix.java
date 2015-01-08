package org.apache.flink.spargel.multicast_als;

import java.util.Random;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class RandomMatrix
		implements
		GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, DoubleVectorWithMap>> {

	private int k;
	private final Random RANDOM = new Random();
	private Tuple2<Integer, DoubleVectorWithMap> vector = new Tuple2<Integer, DoubleVectorWithMap>();
	private DoubleVectorWithMap forValue = new DoubleVectorWithMap();

	public RandomMatrix(int k) {
		this.k = k;
	}

	@Override
	public void reduce(Iterable<Tuple3<Integer, Integer, Double>> elements,
			Collector<Tuple2<Integer, DoubleVectorWithMap>> out)
			throws Exception {
		Tuple3<Integer, Integer, Double> element = elements.iterator().next();

		// create the vertexId of this q column
		int id = IdFormatter.getVertexId(true, element.f1);
		double[] vector_elements = new double[k];
		for (int i = 0; i < k; ++i) {
			vector_elements[i] = 1 + RANDOM.nextDouble() / 2;
		}

		vector.f0 = id;
		forValue.setId(id);
		forValue.setData(vector_elements);
		vector.f1 = forValue;

		// System.out.println(forValue);
		out.collect(vector);
	}
}
