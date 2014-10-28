package org.apache.flink.streaming.api.ft.layer;

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceToFaultToleranceLayerInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;

	private SourceFunction<OUT> sourceFunction;
	private AbstractFaultToleranceLayerCollector<OUT> collector;

	public SourceToFaultToleranceLayerInvokable(SourceFunction<OUT> sourceFunction, AbstractFaultToleranceLayerCollector<OUT> collector) {
		super(sourceFunction);
		this.sourceFunction = sourceFunction;
		this.collector = collector;
	}

	@Override
	public void invoke() throws Exception {
		collector.initialize();
		sourceFunction.invoke(collector);
	}

	@Override
	protected void immutableInvoke() throws Exception {
		// intentionally empty
	}

	@Override
	protected void mutableInvoke() throws Exception {
		// intentionally empty
	}

	@Override
	protected void callUserFunction() throws Exception {
		// intentionally empty
	}

}