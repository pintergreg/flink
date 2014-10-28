package org.apache.flink.streaming.api.ft.layer;

import java.io.Serializable;

import org.apache.flink.util.Collector;

public interface FaultToleranceLayerCollector<T> extends Collector<T>, Serializable {
	public void initialize();
}
