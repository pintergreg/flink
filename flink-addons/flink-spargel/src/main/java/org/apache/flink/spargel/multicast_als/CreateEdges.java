package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CreateEdges
		implements
		FlatMapFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, Integer>> {

	@Override
	public void flatMap(Tuple3<Integer, Integer, Double> value,
			Collector<Tuple2<Integer, Integer>> out) throws Exception {
		int sourceId = IdFormatter.getVertexId(false, value.f0);
		int targetId = IdFormatter.getVertexId(true, value.f1);
		Tuple2<Integer, Integer> directed_edge = new Tuple2(sourceId, targetId);
		Tuple2<Integer, Integer> backward_edge = new Tuple2(targetId, sourceId);
		out.collect(directed_edge);
		out.collect(backward_edge);
	}

}
