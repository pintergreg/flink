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

package org.apache.flink.streaming.api.invokable.operator.window;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class WindowPreAggregator<IN> extends
		StreamInvokable<Tuple3<IN, Integer, Integer>, Tuple2<IN, Integer>> {

	private static final long serialVersionUID = 1L;

	private ReduceFunction<IN> reducer;

	private Integer currentWindowID;
	private LinkedList<IN> buffer;

	Tuple2<IN, Integer> output;

	public WindowPreAggregator(ReduceFunction<IN> reducer) {
		super(reducer);
		this.reducer = reducer;
		this.buffer = new java.util.LinkedList<IN>();
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {

			IN nextElement = nextObject.f0;
			Integer windowID = nextObject.f1;
			Integer numToEvict = nextObject.f2;

			evict(numToEvict);
			emit(windowID);
			addToBuffer(nextElement);

		}
	}

	private void evict(Integer n) {
		for (Integer i = 0; i < n; i++) {
			buffer.remove();
		}
	}

	private void emit(Integer windowID) throws Exception {
		if (currentWindowID != windowID) {
			reduce();
			currentWindowID = windowID;
		}
	}

	private void addToBuffer(IN element) {
		if (element != null) {
			buffer.add(element);
		}
	}

	protected void reduce() throws Exception {
		callUserFunctionAndLogException();

	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<IN> reducedIterator = buffer.iterator();
		IN reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			IN next = reducedIterator.next();
			if (next != null) {
				// TODO: Copy parameters here
				reduced = reducer.reduce(reduced, next);
			}
		}
		if (reduced != null) {
			output.f0 = reduced;
			output.f1 = currentWindowID;

			collector.collect(output);
		}

	}

}
