package org.apache.flink.streaming.api.ft.layer.util;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;

public class ReaderInitializerEvent extends TaskEvent {

	@Override
	public void write(DataOutputView out) throws IOException {

	}

	@Override
	public void read(DataInputView in) throws IOException {

	}
}
