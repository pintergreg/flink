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

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class EdgeListInputFormat extends
		DelimitedInputFormat<Tuple2<Long, long[]>> implements
		ResultTypeQueryable {

	private static final long serialVersionUID = -1945843964517850356L;
	private final IntegerReaderFinal rowReader_ = new IntegerReaderFinal();
	private CsvReader csvReader = new CsvReader();
	private Set<Long> neighbours = new HashSet<Long>();

	public EdgeListInputFormat(String filePath) {
		super();
		if (!filePath.equals("")) {
			this.setFilePath(filePath);
		}
	}

	@Override
	public Tuple2<Long, long[]> readRecord(Tuple2<Long, long[]> record,
			byte[] bytes, int offset, int numBytes) {
		csvReader.startLine(bytes, offset, numBytes);
		record.setField(new Long(csvReader.readInt(rowReader_, '|').f1), 0);

		neighbours.clear();
		while (csvReader.hasMore()) {
			neighbours.add(new Long(csvReader.readInt(rowReader_, '|').f1));
		}

		long[] outgoingEdges = new long[neighbours.size()];
		int index = 0;
		for (long i : neighbours) {
			outgoingEdges[index] = i;
			index++;
		}
		record.setField(outgoingEdges, 1);
		return record;
	}

	@Override
	public TypeInformation<Tuple2<Long, long[]>> getProducedType() {
		return new TupleTypeInfo<Tuple2<Long, long[]>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO);
	}
}
