package org.apache.flink.streaming.api.ft.layer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class SourceFromFaultToleranceLayerInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;

	private FaultToleranceLayerIterator iterator;

	public SourceFromFaultToleranceLayerInvokable(FaultToleranceLayerIterator iterator) {
		super(null);
		this.iterator = iterator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void invoke() throws Exception {
		iterator.initializeFromCurrent();

		while (iterator.hasNext()) {
			MessageWithOffset msg = iterator.nextWithOffset();
			OUT out = (OUT) SerializationUtils.deserialize(msg.getMessage());
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
