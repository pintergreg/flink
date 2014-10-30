/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.acker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.streaming.api.ft.layer.FaultToleranceLayer;
import org.apache.flink.streaming.api.ft.layer.FaultToleranceLayerCollector;
import org.apache.flink.streaming.api.ft.layer.FaultToleranceLayerIterator;
import org.apache.flink.streaming.api.ft.layer.util.RecordWithId;
import org.apache.flink.streaming.api.ft.layer.util.SourceRecordMessage;
import org.apache.flink.streaming.api.ft.layer.util.XorMessage;
import org.junit.Test;

public class AckerTaskTest {

	@Test
	public void ackedTest() {
		FaultToleranceLayer ftLayer = new FaultToleranceLayer();
		AckerTask ackerTask = new AckerTask(ftLayer);

		String rec1 = "abc";
		String rec2 = "xyz";

		FaultToleranceLayerCollector<String> collector = new FaultToleranceLayerCollector<String>(
				ftLayer, 0);
		FaultToleranceLayerIterator<String> iterator = ftLayer.iterator(0);

		RecordWithId<String> rwi1 = pushAndRead(ackerTask, collector, iterator, rec1);
		long id1 = rwi1.getId();
		assertEquals("abc", rwi1.getRecord());

		ackerTask.xor(new XorMessage(30L, id1));
		ackerTask.xor(new XorMessage(35L, id1));

		ackerTask.xor(new XorMessage(id1, id1));

		RecordWithId<String> rwi2 = pushAndRead(ackerTask, collector, iterator, rec2);
		long id2 = rwi2.getId();
		assertEquals("xyz", rwi2.getRecord());

		ackerTask.xor(new XorMessage(40L, id2));

		ackerTask.xor(new XorMessage(id2, id2));

		// records arrive at sink
		
		ackerTask.xor(new XorMessage(30L, id1));
		ackerTask.xor(new XorMessage(35L, id1));

		ackerTask.xor(new XorMessage(40L, id2));

		iterator.initializeFromBeginning();

		assertTrue(!iterator.hasNext());
	}

	private RecordWithId<String> pushAndRead(AckerTask ackerTask, FaultToleranceLayerCollector<String> collector,
			FaultToleranceLayerIterator<String> iterator, String record) {
		collector.collect(record);
		RecordWithId<String> recWithId = iterator.nextWithId();
		ackerTask.newSourceRecord(new SourceRecordMessage(recWithId.getId()));
		return recWithId;
	}

	@Test
	public void failedRecordTest() {
		FaultToleranceLayer ftLayer = new FaultToleranceLayer();
		AckerTask ackerTask = new AckerTask(ftLayer);

		FaultToleranceLayerCollector<String> collector = new FaultToleranceLayerCollector<String>(
				ftLayer, 0);
		FaultToleranceLayerIterator<String> iterator = ftLayer.iterator(0);

		String r1 = "a";
		String r2 = "b";
		String r3 = "c";
		String r4 = "d";

		RecordWithId<String> rwi1 = pushAndRead(ackerTask, collector, iterator, r1);
		long id1 = rwi1.getId();
		assertEquals("a", rwi1.getRecord());
		
		ackerTask.xor(new XorMessage(30L, id1));
		ackerTask.xor(new XorMessage(35L, id1));

		ackerTask.xor(new XorMessage(id1, id1));
		
		RecordWithId<String> rwi2 = pushAndRead(ackerTask, collector, iterator, r2);
		long id2 = rwi2.getId();
		assertEquals("b", rwi2.getRecord());
		
		ackerTask.xor(new XorMessage(112L, id2));
		
		RecordWithId<String> rwi3 = pushAndRead(ackerTask, collector, iterator, r3);
		long id3 = rwi3.getId();
		assertEquals("c", rwi3.getRecord());
		
		ackerTask.xor(new XorMessage(342L, id3));
		
		ackerTask.xor(new XorMessage(id3, id3));

		ackerTask.fail(id2);
		
		assertEquals("b", iterator.next());
		assertEquals("c", iterator.next());
		
		RecordWithId<String> rwi4 = pushAndRead(ackerTask, collector, iterator, r4);
		long id4 = rwi4.getId();
		assertEquals("d", rwi4.getRecord());
			
		ackerTask.xor(new XorMessage(61L, id4));
		ackerTask.xor(new XorMessage(11L, id4));

		ackerTask.xor(new XorMessage(id4, id4));
	}
}
