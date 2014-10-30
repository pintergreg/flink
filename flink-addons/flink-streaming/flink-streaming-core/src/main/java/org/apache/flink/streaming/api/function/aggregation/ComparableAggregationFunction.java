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

package org.apache.flink.streaming.api.function.aggregation;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public abstract class ComparableAggregationFunction<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public ComparableAggregationFunction(int[] positionToAggregate, TypeInformation<?> type) {
		super(positionToAggregate, type);
	}

	@Override
	public T reduce(T value1, T value2) throws Exception {
		return reduce(value1, value2, position, typeInfo);
	}
	
	@SuppressWarnings({ "unchecked" })
	protected T reduce(T field1, T field2, int[] pos,
			TypeInformation<?> typeInfo) throws Exception {
		
		if (pos.length == 1) {			
			if (typeInfo.isTupleType()) {
				Tuple t1 = (Tuple) field1;
				Tuple t2 = (Tuple) field2;
	
				return compare(t1, t2, pos[0]);
			} else if (typeInfo instanceof BasicArrayTypeInfo
					|| typeInfo instanceof PrimitiveArrayTypeInfo) {
				return compareArray(field1, field2, pos[0]);
			} else if (field1 instanceof Comparable) {
				if (isExtremal((Comparable<Object>) field1, field2)) {
					return field1;
				} else {
					return field2;
				}
			} else {
				throw new RuntimeException("The values " + field1 + " and " + field2
						+ " cannot be compared.");
			}
		} else {
			if (typeInfo.isTupleType()) {
				Tuple tuple1 = (Tuple) field1;
				Tuple tuple2 = (Tuple) field2;

				reduceTuple(tuple1, tuple2, pos, typeInfo);
			} else if (typeInfo instanceof BasicArrayTypeInfo
					|| typeInfo instanceof PrimitiveArrayTypeInfo) {
				Object v1 = Array.get(field1, pos[0]);
				Object v2 = Array.get(field2, pos[0]);

				reduceArray(v1, v2, field1, field2, pos, typeInfo);
			}
		}
		return field2;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T reduceTuple(Tuple tuple1, Tuple tuple2, int[] pos, TypeInformation<?> typeInfo) throws Exception {
		Tuple returnTuple = tuple2;
		returnTuple.setField(
				reduce((T) tuple1.getField(pos[0]),
						(T) tuple2.getField(pos[0]),
						Arrays.copyOfRange(pos, 1, pos.length),
						((TupleTypeInfo) typeInfo).getTypeAt(pos[0])),
						pos[0]);
	
		return (T) returnTuple;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public T reduceArray(Object v1, Object v2, T field1, T field2, int[] pos, TypeInformation<?> typeInfo) throws ArrayIndexOutOfBoundsException, IllegalArgumentException, Exception {
		Array.set(field2, pos[0], reduce(
				(T)v1, (T)v2,
				Arrays.copyOfRange(pos, 1, pos.length),
				((BasicArrayTypeInfo) typeInfo).getComponentInfo()
			)
		);
		return field2;
	}

	@SuppressWarnings("unchecked")
	public T compareArray(T array1, T array2, int pos) {
		Object v1 = Array.get(array1, pos);
		Object v2 = Array.get(array2, pos);
		if (isExtremal((Comparable<Object>) v1, v2)) {
			Array.set(array2, pos, v1);
		} else {
			Array.set(array2, pos, v2);
		}

		return array2;
	}

	@SuppressWarnings("unchecked")
	public <R> T compare(Tuple tuple1, Tuple tuple2, int pos) throws InstantiationException,
			IllegalAccessException {

		Object o1 = tuple1.getField(pos);
		Object o2 = tuple2.getField(pos);

		if (isExtremal((Comparable<Object>)o1, o2)) {
			tuple2.setField(o1, pos);
		}
		return (T) tuple2;
	}

	public abstract <R> boolean isExtremal(Comparable<R> o1, R o2);
}
