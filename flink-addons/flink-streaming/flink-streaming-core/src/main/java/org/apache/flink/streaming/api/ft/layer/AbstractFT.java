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

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.streamrecord.IdentifiableStreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public abstract class AbstractFT<T> extends Xorer implements
		Persister<T>, Anchorer {
	protected Persister<T> persister;
	protected Anchorer anchorer;
	protected Xorer xorer;

	public AbstractFT(Persister<T> persister, Xorer xorer, Anchorer
			anchorer) {
		this.persister = persister;
		this.anchorer = anchorer;
		this.xorer = xorer;
	}

	public abstract void fail();

	@Override
	public void setAnchorRecord(IdentifiableStreamRecord anchorRecord) {
		anchorer.setAnchorRecord(anchorRecord);
	}

	@Override
	public RecordId getAnchorRecord() {
		return anchorer.getAnchorRecord();
	}

	@Override
	public RecordId setOutRecordId(SerializationDelegate<? extends IdentifiableStreamRecord>
			outRecord) {
		return anchorer.setOutRecordId(outRecord);
	}

	@Override
	public void xor(IdentifiableStreamRecord record) {
		xorer.xor(record);
		System.out.println("xored id " + record.getId());
	}

	@Override
	public void persist(StreamRecord<T> record) {
		persister.persist(record);
	}

	@Override
	protected void emit(RecordId recordId) throws Exception {
		xorer.emit(recordId);
	}

	@Override
	protected void fail(RecordId recordId) throws Exception {
		xorer.fail(recordId);
	}

	public abstract Collector<T> wrap(Collector<T> collector);

	public abstract boolean faultToleranceIsOn();
}
