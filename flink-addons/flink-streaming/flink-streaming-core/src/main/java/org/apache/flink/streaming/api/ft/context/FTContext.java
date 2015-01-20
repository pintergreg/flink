package org.apache.flink.streaming.api.ft.context;

import org.apache.flink.streaming.api.collector.ft.AckerCollector;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public abstract class FTContext {

	protected AckerCollector ackerCollector;

	abstract public void initialize();

	public void xor(StreamRecord<?> record) {
		ackerCollector.collect(record.getId());
	}

	public void xor(RecordId recordId) {
		ackerCollector.collect(recordId);
	}

	public void setFailFlag(boolean isFailed) {
		ackerCollector.setFailFlag(isFailed);
	}

}
