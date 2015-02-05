/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.function.source;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.util.Collector;

public class FileReadFunction2<T> implements
		FlatMapFunction<Tuple3<String, Long, Long>, Tuple2<String, T>> {

	private static final long serialVersionUID = 1L;
	TypeSerializer<T> serializer;

	public FileReadFunction2(TypeInformation<T> typeInfo) {
		this.serializer = typeInfo.createSerializer();
	}

	@Override
	public void flatMap(Tuple3<String, Long, Long> value, Collector<Tuple2<String, T>> out)
			throws Exception {

		FSDataInputStream stream = FileSystem.get(new URI(value.f0)).open(new Path(value.f0));
		stream.seek(value.f1);

		FileInputView inputView = new FileInputView(stream);

		try {
			while (inputView.available() > 26
					&& (value.f2 == -1L || stream.getPos() <= value.f2)) {

				Tuple2<String, T> output = new Tuple2<String, T>(value.f0,
						serializer.deserialize(inputView));

				out.collect(output);
			}
		} finally {
			stream.close();
		}
	}

	private static class FileInputView extends DataInputStream implements DataInputView {

		public FileInputView(InputStream stream) {
			super(stream);
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}

	}
}
