package org.apache.flink.streaming.api.ft.layer;

import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceFromFaultToleranceLayerInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;

	private AbstractFaultToleranceLayerIterator<OUT> iterator;

	public SourceFromFaultToleranceLayerInvokable(AbstractFaultToleranceLayerIterator<OUT> iterator) {
		super(null);
		this.iterator = iterator;
	}

	@Override
	public void invoke() throws Exception {
		iterator.initializeFromCurrent();

		while (iterator.hasNext()) {
			MessageWithOffset<OUT> msg = iterator.nextWithOffset();
			OUT out = msg.getMessage();
			collector.collect(out);
		}
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
