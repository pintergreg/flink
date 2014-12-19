package org.apache.flink.spargel.multicast_test.io_utils;


import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple2;

public class CsvReader implements Serializable {

    private byte[] line_;
    private int pos_;
    private int actualIndex_;
    private int limit_;

    public void startLine(byte[] line, int offset, int numBytes) {
        line_ = line;
        pos_ = offset;
        actualIndex_ = 0;
        limit_ = offset + numBytes;
    }

    public Tuple2<Integer,Integer> readInt(IntegerReaderFinal idReader, char separator) {
        Tuple2<Integer,Integer> element = new Tuple2<Integer,Integer>();
	idReader.start(element, actualIndex_);
        while (line_[pos_] != separator) {
            idReader.add(line_[pos_]);
            ++pos_;
        }
	idReader.finish();
        ++pos_;
        ++actualIndex_;
	return element;
    }

    public Tuple2<Integer,Double> readDouble(DoubleReaderFinal valueReader, char separator) {
        Tuple2<Integer,Double> element = new Tuple2<Integer,Double>();
	valueReader.start(element, actualIndex_);
        while (line_[pos_] != separator) {
            valueReader.add(line_[pos_]);
            ++pos_;
        }
	valueReader.finish();
        ++pos_;
        ++actualIndex_;
	return element;
    }

    public int getPosition() {
	return pos_;
    }

    public boolean hasMore() {
        return pos_ < limit_;
    }

    public String getLine() {
        return new String(line_).substring(actualIndex_, limit_ - actualIndex_);
    }
}
