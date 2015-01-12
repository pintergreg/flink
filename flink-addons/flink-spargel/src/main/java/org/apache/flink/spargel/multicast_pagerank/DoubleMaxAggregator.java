package org.apache.flink.spargel.multicast_pagerank;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.DoubleValue;


/**
 * An {@link Aggregator} that max-es up {@link DoubleValue} values.
 * Deals with positive numbers.
 * Very similar to the DoubleSumAggregator class.
 */
@SuppressWarnings("serial")
public class DoubleMaxAggregator implements Aggregator<DoubleValue> {

	private DoubleValue wrapper = new DoubleValue();
	private double max;
	
	@Override
	public DoubleValue getAggregate() {
		wrapper.setValue(max);
		return wrapper;
	}

	@Override
	public void aggregate(DoubleValue element) {
		aggregate(element.getValue());
	}

	/**
	 * Adds the given value to the current aggregate.
	 * 
	 * @param value The value to add to the aggregate.
	 */
	public void aggregate(double value) {
		if (max < value) {
			max = value;
		}
	}
	
	@Override
	public void reset() {
		max = 0;
	}
}
