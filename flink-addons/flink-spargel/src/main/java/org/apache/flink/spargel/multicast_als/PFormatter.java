package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class PFormatter
		implements
		GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, DoubleVectorWithMap>> {

	private DoubleVectorWithMap vectorWithMap = new DoubleVectorWithMap();

	@Override
	public void reduce(Iterable<Tuple3<Integer, Integer, Double>> elements,
			Collector<Tuple2<Integer, DoubleVectorWithMap>> out)
			throws Exception {
		Tuple3<Integer, Integer, Double> element = elements.iterator().next();

		// create the vertexId of this p column
		int id = IdFormatter.getVertexId(false, element.f0);
		Tuple2<Integer, DoubleVectorWithMap> vector = new Tuple2<Integer, DoubleVectorWithMap>(
				id, vectorWithMap);
		out.collect(vector);
	}

}