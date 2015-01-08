package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CreateVertices
		implements
		JoinFunction<Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>> {

	private DoubleVectorWithMap value = new DoubleVectorWithMap();
	private Tuple2<Integer, DoubleVectorWithMap> vertex = new Tuple2<Integer, DoubleVectorWithMap>();

	@Override
	public Tuple2<Integer, DoubleVectorWithMap> join(
			Tuple2<Integer, DoubleVectorWithMap> with_data,
			Tuple2<Integer, DoubleVectorWithMap> with_edges) throws Exception {
		value.setId(with_data.f0);
		value.setData(with_data.f1.getData());
		value.setEdges(with_edges.f1.getEdges());
		vertex.setField(with_data.f0, 0);
		vertex.setField(value, 1);
		return vertex;
	}

}