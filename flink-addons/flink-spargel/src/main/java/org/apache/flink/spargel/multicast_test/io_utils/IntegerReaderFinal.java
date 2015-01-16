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

public class IntegerReaderFinal implements ByteReader<Integer> {

	private int value = 0;
	private Tuple2<Integer, Integer> record_;
	private int index_;
	private boolean positive_;

	@Override
	public void start(Tuple2<Integer, Integer> record, int index) {
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
