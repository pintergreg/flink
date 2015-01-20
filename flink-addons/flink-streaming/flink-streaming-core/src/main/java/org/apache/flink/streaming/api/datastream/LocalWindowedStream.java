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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.window.GroupedWindowInvokable;
import org.apache.flink.streaming.api.invokable.operator.window.WindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.window.WindowReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;

public class LocalWindowedStream<OUT> extends WindowedDataStream<OUT> {

	protected LocalWindowedStream(DataStream<OUT> dataStream, WindowingHelper<OUT>[] policyHelpers) {
		super(dataStream, policyHelpers);
	}

	protected LocalWindowedStream(WindowedDataStream<OUT> windowedStream) {
		super(windowedStream);
	}

	/**
	 * Applies a reduce transformation on the windowed data stream by reducing
	 * the current window at every trigger.The user can also extend the
	 * {@link RichReduceFunction} to gain access to other features provided by
	 * the {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reduceFunction) {
		return dataStream.transform("WindowReduce", getType(), getReduceInvokable(reduceFunction));
	}

	/**
	 * Applies a reduceGroup transformation on the windowed data stream by
	 * reducing the current window at every trigger. In contrast with the
	 * standard binary reducer, with reduceGroup the user can access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichGroupReduceFunction} to gain
	 * access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(
			GroupReduceFunction<OUT, R> reduceFunction) {

		TypeInformation<OUT> inType = getType();
		TypeInformation<R> outType = TypeExtractor
				.getGroupReduceReturnTypes(reduceFunction, inType);

		return dataStream.transform("WindowReduce", outType,
				getReduceGroupInvokable(reduceFunction));
	}

	/**
	 * Applies a reduceGroup transformation on the windowed data stream by
	 * reducing the current window at every trigger. In contrast with the
	 * standard binary reducer, with reduceGroup the user can access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichGroupReduceFunction} to gain
	 * access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * </br> </br> This version of reduceGroup uses user supplied
	 * typeinformation for serializaton. Use this only when the system is unable
	 * to detect type information using:
	 * {@link #reduceGroup(GroupReduceFunction)}
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(
			GroupReduceFunction<OUT, R> reduceFunction, TypeInformation<R> outType) {

		return dataStream.transform("WindowReduce", outType,
				getReduceGroupInvokable(reduceFunction));
	}

	private StreamInvokable<OUT, OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
		StreamInvokable<OUT, OUT> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowInvokable<OUT, OUT>(clean(reducer), keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers(),
					getCentralEvicters());

		} else {
			invokable = new WindowReduceInvokable<OUT>(clean(reducer), getTriggers(), getEvicters());
		}
		return invokable;
	}

	private <R> StreamInvokable<OUT, R> getReduceGroupInvokable(GroupReduceFunction<OUT, R> reducer) {
		StreamInvokable<OUT, R> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowInvokable<OUT, R>(clean(reducer), keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers(),
					getCentralEvicters());

		} else {
			invokable = new WindowGroupReduceInvokable<OUT, R>(clean(reducer), getTriggers(),
					getEvicters());
		}
		return invokable;
	}

	@Override
	public LocalWindowedStream<OUT> copy() {
		return new LocalWindowedStream<OUT>(this);
	}
}
