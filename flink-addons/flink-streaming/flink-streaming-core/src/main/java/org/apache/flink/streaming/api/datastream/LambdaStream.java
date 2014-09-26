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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.MapInvokable;
import org.apache.flink.streaming.api.invokable.operator.StreamReduceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

public class LambdaStream<OUT> {

	DataStream<OUT> dataStream;
	DataSet<OUT> dataSet;

	public LambdaStream(DataStream<OUT> dataStream, DataSet<OUT> dataSet) {
		this.dataStream = dataStream;
		this.dataSet = dataSet;
	}

	public <R> SingleOutputStreamOperator<R, ?> map(MapFunction<OUT, R> mapper) {
		FunctionTypeWrapper<OUT> inTypeWrapper = new FunctionTypeWrapper<OUT>(mapper,
				MapFunction.class, 0);
		FunctionTypeWrapper<R> outTypeWrapper = new FunctionTypeWrapper<R>(mapper,
				MapFunction.class, 1);

		SingleOutputStreamOperator<R, ?> returnStream = dataStream.addFunction("map", mapper,
				inTypeWrapper, outTypeWrapper, new MapInvokable<OUT, R>(mapper));
		dataSet.setLambdaID(returnStream.getId());
		return returnStream;
	}

	public <R> SingleOutputStreamOperator<R, ?> flatMap(FlatMapFunction<OUT, R> flatMapper) {
		FunctionTypeWrapper<OUT> inTypeWrapper = new FunctionTypeWrapper<OUT>(flatMapper,
				FlatMapFunction.class, 0);
		FunctionTypeWrapper<R> outTypeWrapper = new FunctionTypeWrapper<R>(flatMapper,
				FlatMapFunction.class, 1);

		SingleOutputStreamOperator<R, ?> returnStream = dataStream
				.addFunction("flatMap", flatMapper, inTypeWrapper, outTypeWrapper,
						new FlatMapInvokable<OUT, R>(flatMapper));
		dataSet.setLambdaID(returnStream.getId());
		return returnStream;
	}

	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {
		SingleOutputStreamOperator<OUT, ?> returnStream = dataStream.addFunction("reduce", reducer,
				new FunctionTypeWrapper<OUT>(reducer, ReduceFunction.class, 0),
				new FunctionTypeWrapper<OUT>(reducer, ReduceFunction.class, 0),
				new StreamReduceInvokable<OUT>(reducer));
		dataSet.setLambdaID(returnStream.getId());
		return returnStream;
	}

}
