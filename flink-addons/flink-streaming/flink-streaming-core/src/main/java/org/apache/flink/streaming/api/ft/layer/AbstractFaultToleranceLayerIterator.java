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

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.streaming.api.ft.layer.util.RecordWithId;

public abstract class AbstractFaultToleranceLayerIterator<T> implements Iterator<T>, Serializable {

	private static final long serialVersionUID = 1L;

	public abstract void reset(long offset);
	public abstract RecordWithId<T> nextWithId();
	
	public abstract void initializeFromBeginning();
	public abstract void initializeFromCurrent();
	public abstract void initializeFromOffset(long offset);

	@Override
	public void remove() {
		throw new RuntimeException("Cannot remove message from queue.");
	}
	
	public abstract long getLastOffset();

	public abstract long currentOffset();
}