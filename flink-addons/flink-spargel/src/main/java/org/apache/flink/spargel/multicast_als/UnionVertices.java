package org.apache.flink.spargel.multicast_als;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//TODO: from some reason the union operator for datasets does not work so this object works around this problem temporarily!

public class UnionVertices
		implements
		CoGroupFunction<Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, DoubleVectorWithMap>> {

	private Tuple2<Integer, DoubleVectorWithMap> output = new Tuple2<Integer, DoubleVectorWithMap>();

	@Override
	public void coGroup(Iterable<Tuple2<Integer, DoubleVectorWithMap>> fromP_,
			Iterable<Tuple2<Integer, DoubleVectorWithMap>> fromQ_,
			Collector<Tuple2<Integer, DoubleVectorWithMap>> out)
			throws Exception {

		Iterator<Tuple2<Integer, DoubleVectorWithMap>> fromP = fromP_
				.iterator();
		Iterator<Tuple2<Integer, DoubleVectorWithMap>> fromQ = fromQ_
				.iterator();

		if (fromP.hasNext()) {
			Tuple2<Integer, DoubleVectorWithMap> pVertex = fromP.next();
			DoubleVectorWithMap forP = pVertex.f1;
			// forP.turnOffEdgeSending();
			out.collect(new Tuple2<Integer, DoubleVectorWithMap>(pVertex.f0,
					forP));

		} else if (fromQ.hasNext()) {
			Tuple2<Integer, DoubleVectorWithMap> qVertex = fromQ.next();
			DoubleVectorWithMap forQ = qVertex.f1;
			// forQ.turnOffEdgeSending();
			out.collect(new Tuple2<Integer, DoubleVectorWithMap>(qVertex.f0,
					forQ));
		} else {
			throw new RuntimeException("Both iterators are empty!");
		}

	}

}
