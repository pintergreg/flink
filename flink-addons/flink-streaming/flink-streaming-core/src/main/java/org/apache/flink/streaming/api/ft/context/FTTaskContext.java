package org.apache.flink.streaming.api.ft.context;

import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.streaming.api.collector.ft.TaskAckerCollector;

public class FTTaskContext extends FTContext {

	public FTTaskContext(MutableRecordReader<?> persistenceInput) {
		ackerCollector = new TaskAckerCollector(persistenceInput);
	}

	@Override
	public void initialize() {

	}

}
