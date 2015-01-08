package org.apache.flink.spargel.multicast_test.io_utils;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple2;

public interface ByteReader<T extends Number> extends Serializable {

	public void start(Tuple2<Integer, T> record, int index);

	public void add(byte data);

	public void finish();

}
