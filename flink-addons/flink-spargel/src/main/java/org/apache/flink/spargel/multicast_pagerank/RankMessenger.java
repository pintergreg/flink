package org.apache.flink.spargel.multicast_pagerank;


import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.NullValue;

/**
 * Distributes the rank of a vertex among all target vertices according to the transition probability,
 * which is associated with an edge as the edge value.
 */
public class RankMessenger extends MessagingFunction<Long, SpargelNode, Double, NullValue> {

	private static final long serialVersionUID = 1L;
	private DoubleSumAggregator agg;

	// This is needed for the epsilon filter
	private DoubleValue maxChangeValueInPrevIter;
	private double epsilonForConvergence; 

	public RankMessenger(double epsilonForConvergence) {
		this.epsilonForConvergence = epsilonForConvergence;
	}
	
	@Override
	public void preSuperstep() {
		agg = getIterationAggregator(PageRankUtil.VALUE_COLLECTED_BY_SINKS);
		//agg.reset();
		if ((getSuperstepNumber() > 1) && (getSuperstepNumber() % 2 == 1)) {
			maxChangeValueInPrevIter = getPreviousIterationAggregate(PageRankUtil.MAX_RANK_CHANGE);
			//System.out.println("Max change in previous 2 iterations: " + maxChangeValueInPrevIter.getValue());
		}

	}

	@Override
	public void sendMessages(Long vertexId, SpargelNode vertexValue) {
		if (getSuperstepNumber() % 2 == 1) {
			if (getSuperstepNumber() > 1) {
				//We check the convergence criterion
				if (maxChangeValueInPrevIter.getValue() < epsilonForConvergence){
					// no vertex updates will be done
					return;
				}
			}

			for (OutgoingEdge<Long, NullValue> edge : getOutgoingEdges()) {
				sendMessageTo(edge.target(), vertexValue.getRank()
						/ vertexValue.getOutDegree());
			}
			if (vertexValue.isSource) {
				sendMessageTo(vertexId, 0.0);
			}
			if (vertexValue.getOutDegree() == 0) {
				// deal with the sinks
				// agg = getIterationAggregator("valueCollectedBySinks");
				// System.out.println("Superstep: " + getSuperstepNumber() +
				// ", "
				// + n.getCurrentPageRank());
				agg.aggregate(vertexValue.getRank());
			}

		} else {
			// in every second round we send a dummy message to everyone
			// to wake them up and update the value sent by the sinks
			sendMessageTo(vertexId, 0.0);
		}

	}
}
