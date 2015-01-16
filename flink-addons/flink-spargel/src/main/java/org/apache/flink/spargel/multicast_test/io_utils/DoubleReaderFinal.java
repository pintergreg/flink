/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.spargel.multicast_test.io_utils;

import org.apache.flink.api.java.tuple.Tuple2;

public class DoubleReaderFinal implements ByteReader<Double> {

	private int intValue = 0;
	private int fracValue = 0;
	private int fracChars = 0;
	private Tuple2<Integer, Double> record_;
	private int index_;
	private boolean positive_;

	@Override
	public void start(Tuple2<Integer, Double> record, int index) {
		intValue = 0;
		fracValue = 0;
		fracChars = 0;
		positive_ = true;
		record_ = record;
		index_ = index;
	}

	@Override
	public void add(byte data) {
		if (data == '-') {
			positive_ = false;
		} else if (data == '.') {
			fracChars = 1;
		} else {
			if (fracChars == 0) {
				intValue *= 10;
				intValue += data - '0';
			} else {
				fracValue *= 10;
				fracValue += data - '0';
				fracChars++;
			}
		}
	}

	@Override
	public void finish() {
		double value = intValue + ((double) fracValue)
				* Math.pow(10, (-1 * (fracChars - 1)));
		double result = (positive_ ? value : -value);
		// record_.setFields(index_, DoubleFormatter.format(result));//TODO:
		// double decimal format!
		record_.setFields(index_, result);
	}
}
