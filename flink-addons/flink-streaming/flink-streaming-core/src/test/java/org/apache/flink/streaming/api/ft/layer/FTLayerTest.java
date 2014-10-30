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

package org.apache.flink.streaming.api.ft.layer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.flink.streaming.api.ft.layer.util.RecordWithId;
import org.junit.Test;

public class FTLayerTest {

	@Test
	public void ftLayerTest() {
		FaultToleranceLayer ftLayer = new FaultToleranceLayer();
		FaultToleranceLayerCollector<String> coll = new FaultToleranceLayerCollector<String>(
				ftLayer, 100);
		FaultToleranceLayerIterator<String> iter = ftLayer.iterator(100);

		coll.collect("one");
		coll.collect("two");
		coll.collect("three");
		
		try {
			iter.reset(10);
			fail();
		} catch (RuntimeException e) {
		}

		FaultToleranceLayerCollector<Integer> coll2 = new FaultToleranceLayerCollector<Integer>(
				ftLayer, 200);

		coll2.collect(11);

		coll.collect("four");
		coll.collect("five");

		coll2.collect(15);

		ArrayList<String> expectedResult = new ArrayList<String>();
		expectedResult.add("one");
		expectedResult.add("two");
		expectedResult.add("three");
		expectedResult.add("four");
		expectedResult.add("five");
		
		ArrayList<String> actualResult = new ArrayList<String>();
		
		while (iter.hasNext()) {
			actualResult.add(iter.next());
		}

		assertEquals(expectedResult, actualResult);
		
		iter.reset(2);
		RecordWithId<String> msg = iter.nextWithId();
		
//		assertEquals(2, msg.getOffset());
		assertEquals("three", msg.getRecord());

		coll2.collect(2);

		FaultToleranceLayerIterator<Integer> iter2 = ftLayer.iterator(200);
		iter2.reset(1);

		ArrayList<Integer> expectedResult2 = new ArrayList<Integer>();
		expectedResult2.add(15);
		expectedResult2.add(2);
		
		ArrayList<Integer> actualResult2 = new ArrayList<Integer>();
		
		while (iter2.currentOffset() <= iter2.getLastOffset()) {
			actualResult2.add(iter2.next());
		}
	}

}
