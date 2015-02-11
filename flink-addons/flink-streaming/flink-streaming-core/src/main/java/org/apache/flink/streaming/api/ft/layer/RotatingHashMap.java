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

package org.apache.flink.streaming.api.ft.layer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.streaming.api.ft.layer.util.ExpiredFunction;

public class RotatingHashMap<K, V> implements AbstractPersistenceStorage<K, V> {

	private LinkedList<Map<K, V>> buckets;
	private ExpiredFunction<K, V> expiredFunction;
	private int numberOfBuckets;

	public RotatingHashMap(ExpiredFunction<K, V> expiredFunction, int numberOfBuckets) {
		this.expiredFunction = expiredFunction;
		this.numberOfBuckets = numberOfBuckets;

		buckets = new LinkedList<Map<K, V>>();

		for (int i = 0; i < numberOfBuckets; i++) {
			buckets.add(new HashMap<K, V>());
		}
	}

	public V get(K key) {
		for (Map<K, V> bucket : buckets) {
			if (bucket.containsKey(key)) {
				return bucket.get(key);
			}
		}
		return null;
	}

	@Override
	public boolean contains(K key) {
		for (Map<K, V> bucket : buckets) {
			if (bucket.containsKey(key)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void remove(K key) {
		for (Map<K, V> bucket : buckets) {
			bucket.remove(key);
		}
	}

	public void put(K key, V value) {
		Map<K, V> currentBucket = buckets.getFirst();
		currentBucket.put(key, value);
	}

	public void rotate() {
		Map<K, V> eldestBucket = buckets.removeLast();

		for (Map.Entry<K, V> entry : eldestBucket.entrySet()) {
			expiredFunction.onExpire(entry.getKey(), entry.getValue());
		}

		buckets.addFirst(new HashMap<K, V>());
	}
}
