package org.apache.flink.spargel.java.multicast;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

public class UnpackMsgsWithRecipientsMC1<VertexKey extends Comparable<VertexKey>, Message>
		extends
		RichFlatMapFunction<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>> {

	private static final long serialVersionUID = 1L;
	private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;

	public UnpackMsgsWithRecipientsMC1(
			TypeInformation<Tuple2<VertexKey, Message>> resultType) {
		this.resultType = resultType;
	}

	Tuple2<VertexKey, Message> reuse = new Tuple2<VertexKey, Message>();

	@Override
	public void flatMap(
			Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>> value,
			Collector<Tuple2<VertexKey, Message>> out) throws Exception {
		reuse.f1 = value.f1.getMessage();
		for (VertexKey target : value.f1.getSomeRecipients()) {
			reuse.f0 = target;
			out.collect(reuse);
		}

	}

	@Override
	public TypeInformation<Tuple2<VertexKey, Message>> getProducedType() {
		return this.resultType;
	}
}
