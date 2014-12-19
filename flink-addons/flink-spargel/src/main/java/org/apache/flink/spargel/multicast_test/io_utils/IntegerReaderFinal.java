package org.apache.flink.spargel.multicast_test.io_utils;

import org.apache.flink.api.java.tuple.Tuple2;

public class IntegerReaderFinal implements ByteReader<Integer> {

  private int value = 0;
  private Tuple2<Integer,Integer> record_;
  private int index_;
  private boolean positive_;

  @Override
  public void start(Tuple2<Integer,Integer> record, int index) {
    value = 0;
    record_ = record;
    index_ = index;
    positive_ = true;
  }

  @Override
  public void add(byte data) {
    if (data == '-') {
      positive_ = false;
    } else {
      value *= 10;
      value += data - '0';
    }
  }

  @Override
  public void finish() {
    final int result_ = (positive_ ? value : -value);
    record_.setFields(index_, result_);
  }
  
}
