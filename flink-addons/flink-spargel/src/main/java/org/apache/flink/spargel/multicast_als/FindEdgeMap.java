package org.apache.flink.spargel.multicast_als;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class FindEdgeMap
		implements
		GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, DoubleVectorWithMap>> {

	private int index;
	private HashMap<String, Double> edges = new HashMap<String, Double>();
	private DoubleVectorWithMap vector = new DoubleVectorWithMap();

	public FindEdgeMap(int index) {
		this.index = index;
	}

	@Override
	public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values_,
			Collector<Tuple2<Integer, DoubleVectorWithMap>> out)
			throws Exception {

		Iterator<Tuple3<Integer, Integer, Double>> values = values_.iterator();

		edges.clear();
		int originalId = -1;
		int otherId = -1;

		while (values.hasNext()) {
			Tuple3<Integer, Integer, Double> element = values.next();
			originalId = (index == 0 ? IdFormatter.getVertexId(false,
					element.f0) : IdFormatter.getVertexId(true, element.f1));
			otherId = (index == 0 ? IdFormatter.getVertexId(true, element.f1)
					: IdFormatter.getVertexId(false, element.f0));
			edges.put(Integer.toString(otherId), element.f2);
		}

		if (originalId == -1) {
			throw new RuntimeException(
					"The originalId was not assigned a value");
		}
		if (otherId == -1) {
			throw new RuntimeException("The otherId was not assigned a value");
		}

		vector.setId(originalId);
		vector.setEdges(edges);
		out.collect(new Tuple2<Integer, DoubleVectorWithMap>(originalId, vector));
	}

}
