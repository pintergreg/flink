package org.apache.flink.spargel.java;

import java.util.Iterator;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.spargel.java.multicast.MCEnum;
import org.apache.flink.spargel.java.multicast.MessageWithHeader;
import org.apache.flink.util.Collector;

/*
 * UDF that encapsulates the message sending function for graphs where the
 * edges have no associated values.
 */
public final class MessagingUdfNoEdgeValuesMC<VertexKey extends Comparable<VertexKey>, VertexValue, Message>
		extends
		RichCoGroupFunction<Tuple2<VertexKey, VertexKey>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>>
		implements
		ResultTypeQueryable<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> {
	private static final long serialVersionUID = 1L;

	private final MessagingFunction3<VertexKey, VertexValue, Message, ?> messagingFunction;

	private transient TypeInformation<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> resultType;

	private MCEnum whichMulticast;
	
	// does this have to know whichMulticast?
	MessagingUdfNoEdgeValuesMC(
			MessagingFunction3<VertexKey, VertexValue, Message, ?> messagingFunction,
			TypeInformation<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> resultType, 
			MCEnum whichMulticast) {
		this.messagingFunction = messagingFunction;
		this.resultType = resultType;
		this.whichMulticast = whichMulticast;
		if (this.whichMulticast != messagingFunction.getWhichMulticast()) {
			throw new RuntimeException("The multicast id for the messagingFunction and MessagingUdfNoEdgeValuesMC should be equal.");
		}
	}

	@Override
	public void coGroup(
			Iterable<Tuple2<VertexKey, VertexKey>> edges,
			Iterable<Tuple2<VertexKey, VertexValue>> state,
			Collector<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> out)
			throws Exception {
		final Iterator<Tuple2<VertexKey, VertexValue>> stateIter = state
				.iterator();

		if (stateIter.hasNext()) {
			Tuple2<VertexKey, VertexValue> newVertexState = stateIter
					.next();

			messagingFunction.setMC((Iterator<?>) edges.iterator(), out);
			messagingFunction.setSender(newVertexState.f0);
			messagingFunction.sendMessages(newVertexState.f0,
					newVertexState.f1);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
			this.messagingFunction
					.init(getIterationRuntimeContext(), false);
		}

		this.messagingFunction.preSuperstep();
	}

	@Override
	public void close() throws Exception {
		this.messagingFunction.postSuperstep();
	}

	@Override
	public TypeInformation<Tuple2<VertexKey, MessageWithHeader<VertexKey, Message>>> getProducedType() {
		return this.resultType;
	}
}
