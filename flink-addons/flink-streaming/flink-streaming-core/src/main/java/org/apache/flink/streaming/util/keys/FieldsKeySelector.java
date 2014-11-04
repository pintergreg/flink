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

package org.apache.flink.streaming.util.keys;

import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.reducefunction.NestedAggregate;
import org.apache.flink.streaming.api.reducefunction.NestedAggregate.Aggregation;

public class FieldsKeySelector<IN> implements KeySelector<IN, Object> {

	private static final long serialVersionUID = 1L;

	int[] keyFields;
	boolean isTuple;
	boolean isArray;
	int numberOfKeys;
	Object key;

	public static Class<?>[] tupleClasses = new Class[] { Tuple1.class, Tuple2.class, Tuple3.class,
			Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class,
			Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class, Tuple14.class,
			Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class, Tuple19.class,
			Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class, Tuple24.class,
			Tuple25.class };

	public FieldsKeySelector(boolean isTuple, boolean isArray, int... fields) {
		this.keyFields = fields;
		this.numberOfKeys = fields.length;
		this.isTuple = isTuple;
		this.isArray = isArray;

		for (int i : fields) {
			if (i < 0) {
				throw new RuntimeException("Grouping fields must be non-negative");
			}
		}

		if (numberOfKeys > 1) {
			if (!this.isTuple && !this.isArray) {
				throw new RuntimeException(
						"For non-tuple types use single field 0 or KeyExctractor for grouping");
			} else {
				try {
					key = tupleClasses[fields.length - 1].newInstance();
				} catch (Exception e) {
					throw new RuntimeException(e.getMessage());
				}
			}
		} else {
			if (!this.isTuple && !this.isArray) {
				if (fields[0] > 0) {
					throw new RuntimeException(
							"For simple objects grouping only allowed on the first field");
				}
			}
			key = null;
		}
	}

	public FieldsKeySelector(TypeInformation<IN> type, int... fields) {
		this(type.isTupleType(),
				(type instanceof BasicArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo),
				fields);
	}

	@Override
	public Object getKey(IN value) throws Exception {
		if (numberOfKeys > 1) {
			int c = 0;
			if (isTuple) {
				for (int pos : keyFields) {
					((Tuple) key).setField(((Tuple) value).getField(pos), c);
					c++;
				}
			} else {
				// if array type
				for (int pos : keyFields) {
					((Tuple) key).setField(Array.get(value, pos), c);
					c++;
				}

			}
		} else {
			if (isTuple) {
				key = ((Tuple) value).getField(keyFields[0]);
			} else if (isArray) {
				key = Array.get(value, keyFields[0]);
			} else {
				key = value;
			}
		}
		return key;
	}
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		@SuppressWarnings("unchecked")
		DataStream<Tuple3<Integer, Tuple2<Integer[], Integer[]>, Integer>> d = env.fromElements(new Tuple3<Integer, Tuple2<Integer[], Integer[]>, Integer> (1, new Tuple2<Integer[], Integer[]> (new Integer[]{2,3}, new Integer[]{4,5, 6}), 7), new Tuple3<Integer, Tuple2<Integer[], Integer[]>, Integer> (8, new Tuple2<Integer[], Integer[]> (new Integer[]{9}, new Integer[]{10,11,12}), 13));
		
		d.reduce(new NestedAggregate(Aggregation.minBy, 1, 1, 1)).print();
		d.reduce(new NestedAggregate(Aggregation.max, 1, 1, 0)).print();
		d.reduce(new NestedAggregate(Aggregation.sum, 1, 1, 2)).print();
		
		ArrayList<Tuple2<Integer, Integer[]>> aList = new ArrayList<Tuple2<Integer, Integer[]>>();
		aList.add(new Tuple2<Integer, Integer[]>(1, new Integer[]{5,3}));
		aList.add(new Tuple2<Integer, Integer[]>(2, new Integer[]{4,2}));
		aList.add(new Tuple2<Integer, Integer[]>(3, new Integer[]{3,5}));
		aList.add(new Tuple2<Integer, Integer[]>(4, new Integer[]{2,1}));
		aList.add(new Tuple2<Integer, Integer[]>(5, new Integer[]{1,4}));
		
		DataStream<Tuple2<Integer, Integer[]>> d2 = env.fromCollection(aList);
		
		d2.reduce(new NestedAggregate(Aggregation.minBy, 1, 1)).print();
		d2.reduce(new NestedAggregate(Aggregation.max, 0)).print();
		d2.reduce(new NestedAggregate(Aggregation.sum, 1, 0)).print();
		
		env.execute();
	}

}
