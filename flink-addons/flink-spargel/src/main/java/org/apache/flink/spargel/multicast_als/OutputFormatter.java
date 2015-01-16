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
package org.apache.flink.spargel.multicast_als;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class OutputFormatter
		implements
		GroupReduceFunction<Tuple2<Integer, DoubleVectorWithMap>, Tuple2<Integer, double[]>> {

	private boolean partOfQ;
	private int modulo;

	public OutputFormatter(boolean partOfQ) {
		this.partOfQ = partOfQ;
	}

	@Override
	public void reduce(Iterable<Tuple2<Integer, DoubleVectorWithMap>> records,
			Collector<Tuple2<Integer, double[]>> out) throws Exception {
		modulo = partOfQ ? 0 : 1;

		Tuple2<Integer, DoubleVectorWithMap> record = records.iterator().next();
		DoubleVectorWithMap value = record.f1;

		// TODO: the exception is commented out only for TESTs
		if (value.getData() == null) {
			// throw new
			// NullPointerException("The data of DoubleVectorWithMap is null!");
			value.setData(new double[0]);
		}

		// collect only the requested partition
		if (value.getId() % 2 == modulo) {
			out.collect(new Tuple2(IdFormatter.getOriginalId(value.getId()),
					value.getData()));
		}
	}

}