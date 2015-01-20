package org.apache.flink.streaming.api.ft.context;

import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class NoFTContext extends FTContext {

	@Override
	public void initialize() {

	}

	public void xor(StreamRecord<?> record) {

	}

	public void xor(RecordId recordId) {

	}

	public void setFailFlag(boolean isFailed) {

	}

}
