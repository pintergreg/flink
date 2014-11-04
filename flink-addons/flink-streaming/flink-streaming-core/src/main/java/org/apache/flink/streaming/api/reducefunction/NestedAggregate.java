package org.apache.flink.streaming.api.reducefunction;

public class NestedAggregate {
	public enum Aggregation { 
		sum, min, max, 
		minBy, minBy_true, minBy_false, 		
		maxBy, maxBy_true, maxBy_false
	}
	private final Aggregation aggregation;
	private final int[] positions;
	
	public NestedAggregate(Aggregation aggregation, int... positions) {
		this.aggregation = aggregation;
		this.positions = positions;
	}
	
	public int[] getPositions() {
		return positions;
	}
	
	public Aggregation getFunction() {
		return aggregation;
	}
}
