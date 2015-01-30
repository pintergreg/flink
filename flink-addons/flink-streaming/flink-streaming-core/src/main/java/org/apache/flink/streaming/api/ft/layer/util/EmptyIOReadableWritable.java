package org.apache.flink.streaming.api.ft.layer.util;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class EmptyIOReadableWritable implements IOReadableWritable {

	@Override
	public void write(DataOutputView out) throws IOException {

	}

	@Override
	public void read(DataInputView in) throws IOException {

	}

}
