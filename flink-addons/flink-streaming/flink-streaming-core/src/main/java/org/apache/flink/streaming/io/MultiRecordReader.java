package org.apache.flink.streaming.io;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;

public class MultiRecordReader<T extends IOReadableWritable> extends MutableRecordReader<T> implements MutableReader<T> {

	MultiBufferReaderBase multiReader;

	public MultiRecordReader(MultiBufferReaderBase reader) {
		super(reader);
		this.multiReader = reader;
	}

	public int nextRecordWithIndex(final T target) throws IOException, InterruptedException {
		boolean hasNext = getNextRecord(target);
		return hasNext ? multiReader.getNextReaderIndex() : -1;
	}

	@Override
	public void clearBuffers() {
		super.clearBuffers();
	}
}

