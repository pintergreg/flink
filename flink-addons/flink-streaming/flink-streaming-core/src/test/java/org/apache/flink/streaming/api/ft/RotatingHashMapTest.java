/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft;

import org.apache.flink.streaming.api.ft.layer.RotatingHashMap;
import org.apache.flink.streaming.api.ft.layer.util.ExpiredFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RotatingHashMapTest {

	private static ExpiredFunction<Long, String> expiredFunction;
	private static int numberOfBuckets;
	private static MockRotatingHashMap rotatingHashMap;

	private static Map<Long, String> expiredRecords;

	private static class MockRotatingHashMap extends RotatingHashMap<Long, String> {

		public MockRotatingHashMap(ExpiredFunction<Long, String> expiredFunction, int numberOfBuckets) {
			super(expiredFunction, numberOfBuckets);
		}

		public void rotate() {
			expiredRecords.clear();
			super.rotate();
		}

		public int whichBucket(Long key) {
			for (int i = 0; i < buckets.size(); i++) {
				if (buckets.get(i).containsKey(key)) {
					return i;
				}
			}
			return -1;
		}

		public boolean isEmpty() {
			boolean isEmpty = true;
			for (Map<Long, String> bucket : buckets) {
				if (!bucket.isEmpty()) {
					isEmpty = false;
				}
			}
			return isEmpty;
		}
	}

	private static class MyExpiredFunction implements ExpiredFunction<Long, String> {

		@Override
		public void onExpire(Long key, String value) {
			expiredRecords.put(key, value);
		}
	}

	@Test
	public void rotatingHashMapTest() {

		expiredFunction = new MyExpiredFunction();
		numberOfBuckets = 6;
		rotatingHashMap = new MockRotatingHashMap(expiredFunction, numberOfBuckets);

		expiredRecords = new HashMap<Long, String>();

		rotatingHashMap.put(1L, "one");
		rotatingHashMap.put(2L, "two");
		rotatingHashMap.put(3L, "three");
		rotatingHashMap.put(4L, "four");
		assertEquals(0, rotatingHashMap.whichBucket(1L));
		assertEquals(0, rotatingHashMap.whichBucket(2L));
		assertEquals(0, rotatingHashMap.whichBucket(3L));
		assertEquals(0, rotatingHashMap.whichBucket(4L));
		assertEquals(0, expiredRecords.size());

		rotatingHashMap.rotate();
		assertEquals(1, rotatingHashMap.whichBucket(1L));
		assertEquals(1, rotatingHashMap.whichBucket(2L));
		assertEquals(1, rotatingHashMap.whichBucket(3L));
		assertEquals(1, rotatingHashMap.whichBucket(4L));
		assertEquals(0, expiredRecords.size());

		rotatingHashMap.put(5L, "five");
		rotatingHashMap.put(6L, "six");
		rotatingHashMap.rotate();
		assertEquals(2, rotatingHashMap.whichBucket(1L));
		assertEquals(2, rotatingHashMap.whichBucket(2L));
		assertEquals(2, rotatingHashMap.whichBucket(3L));
		assertEquals(2, rotatingHashMap.whichBucket(4L));
		assertEquals(1, rotatingHashMap.whichBucket(5L));
		assertEquals(1, rotatingHashMap.whichBucket(6L));
		assertEquals(0, expiredRecords.size());

		assertEquals("one", rotatingHashMap.get(1L));
		rotatingHashMap.remove(1L);
		assertEquals("two", rotatingHashMap.get(2L));
		rotatingHashMap.remove(2L);
		rotatingHashMap.put(7L, "seven");
		rotatingHashMap.rotate();
		assertEquals(-1, rotatingHashMap.whichBucket(1L));
		assertEquals(-1, rotatingHashMap.whichBucket(2L));
		assertEquals(3, rotatingHashMap.whichBucket(3L));
		assertEquals(3, rotatingHashMap.whichBucket(4L));
		assertEquals(2, rotatingHashMap.whichBucket(5L));
		assertEquals(2, rotatingHashMap.whichBucket(6L));
		assertEquals(1, rotatingHashMap.whichBucket(7L));
		assertEquals(0, expiredRecords.size());

		rotatingHashMap.put(8L, "eight");
		assertEquals("four", rotatingHashMap.get(4L));
		rotatingHashMap.remove(4L);
		rotatingHashMap.put(9L, "nine");
		assertEquals("five", rotatingHashMap.get(5L));
		rotatingHashMap.remove(5L);
		rotatingHashMap.put(10L, "ten");
		rotatingHashMap.rotate();
		assertEquals(4, rotatingHashMap.whichBucket(3L));
		assertEquals(-1, rotatingHashMap.whichBucket(4L));
		assertEquals(-1, rotatingHashMap.whichBucket(5L));
		assertEquals(3, rotatingHashMap.whichBucket(6L));
		assertEquals(2, rotatingHashMap.whichBucket(7L));
		assertEquals(1, rotatingHashMap.whichBucket(8L));
		assertEquals(1, rotatingHashMap.whichBucket(9L));
		assertEquals(1, rotatingHashMap.whichBucket(10L));
		assertEquals(0, expiredRecords.size());

		assertEquals("seven", rotatingHashMap.get(7L));
		rotatingHashMap.remove(7L);
		rotatingHashMap.rotate();
		assertEquals(5, rotatingHashMap.whichBucket(3L));
		assertEquals(4, rotatingHashMap.whichBucket(6L));
		assertEquals(-1, rotatingHashMap.whichBucket(7L));
		assertEquals(2, rotatingHashMap.whichBucket(8L));
		assertEquals(2, rotatingHashMap.whichBucket(9L));
		assertEquals(2, rotatingHashMap.whichBucket(10L));
		assertEquals(0, expiredRecords.size());

		assertEquals("eight", rotatingHashMap.get(8L));
		rotatingHashMap.remove(8L);
		rotatingHashMap.rotate();
		assertEquals(-1, rotatingHashMap.whichBucket(3L));
		assertEquals("three", expiredRecords.get(3L));
		assertEquals(5, rotatingHashMap.whichBucket(6L));
		assertEquals(-1, rotatingHashMap.whichBucket(8L));
		assertEquals(3, rotatingHashMap.whichBucket(9L));
		assertEquals(3, rotatingHashMap.whichBucket(10L));
		assertEquals(1, expiredRecords.size());

		rotatingHashMap.rotate();
		assertEquals(-1, rotatingHashMap.whichBucket(6L));
		assertEquals("six", expiredRecords.get(6L));
		assertEquals(4, rotatingHashMap.whichBucket(9L));
		assertEquals(4, rotatingHashMap.whichBucket(10L));
		assertEquals(1, expiredRecords.size());

		rotatingHashMap.rotate();
		assertEquals(5, rotatingHashMap.whichBucket(9L));
		assertEquals(5, rotatingHashMap.whichBucket(10L));
		assertEquals(0, expiredRecords.size());

		rotatingHashMap.rotate();
		assertEquals(-1, rotatingHashMap.whichBucket(9L));
		assertEquals("nine", expiredRecords.get(9L));
		assertEquals(-1, rotatingHashMap.whichBucket(10L));
		assertEquals("ten", expiredRecords.get(10L));
		assertEquals(2, expiredRecords.size());

		assertTrue(rotatingHashMap.isEmpty());

	}
}
