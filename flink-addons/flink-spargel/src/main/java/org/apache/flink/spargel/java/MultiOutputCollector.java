package org.apache.flink.spargel.java;

import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;


public class MultiOutputCollector<T> extends org.apache.flink.runtime.operators.shipping.OutputCollector<T> {

	public MultiOutputCollector(
			List<RecordWriter<SerializationDelegate<T>>> writers,
			TypeSerializer<T> serializer) {
		super(writers, serializer);
		if (writers.size() > 1) {
			throw new 
			 UnsupportedOperationException("The number of writers should be 1");
		}
	}
	
	
	
}
