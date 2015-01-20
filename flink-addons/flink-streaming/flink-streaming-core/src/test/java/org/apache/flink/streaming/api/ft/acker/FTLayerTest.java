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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.util.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.invokable.ft.FTLayerVertex;
import org.junit.Test;

public class FTLayerTest {

	private MockFTLayer ftLayer;
	private HashSet<Integer> tasks;
	private HashSet<Integer> sources;
	private HashSet<Integer> sinks;
	private List<Integer> executionOrder;
	//	private HashMap<Integer, ArrayList<RecordId>> replayed;
	private HashMap<Integer, ArrayList<Integer>> topology;
	private HashMap<Integer, ArrayList<RecordId>> inputLists;
	private HashMap<Integer, ArrayList<SemiDeserializedStreamRecord>> sourceInputLists;
	private HashMap<Integer, HashSet<Integer>> recordsToFail;
	private HashMap<Integer, ArrayList<RecordId>> failedRecordLists;
	private HashMap<Integer, Integer> counters;
	private boolean failed;
	private long failedId;
	private byte[] failedObject;

	private class MockFTLayer extends FTLayer {

		public MockFTLayer(FTLayerVertex ftLayerInvokable) {
			super(ftLayerInvokable);
		}

		@Override
		protected void collectToSourceSuccessiveTasks(int sourceId,
				SemiDeserializedStreamRecord sourceRecord) {
			assertEquals(ackerTable.contains(failedId), false);
			assertEquals(persistenceLayer.get(sourceRecord.getId().getSourceRecordId()), failedObject);
			ArrayList<Integer> downstreamTasks = topology.get(sourceId);
			for (int task : downstreamTasks) {
				RecordId newId = RecordId.newRecordId(sourceRecord.getId().getSourceRecordId());
				ftLayer.xor(newId);
				failedRecordLists.get(task).add(newId);
				assertEquals(ftLayer.isFinished(), false);
			}
			// replayed.get(sourceId).add(sourceRecord.getId());
		}

		public boolean isFinished() {
			return ackerTable.isEmpty();
		}

		public byte[] getSerializedObject(long failedId) {
			return persistenceLayer.get(failedId).getSerializedRecord();
		}

	}

	//	private ArrayList<RecordId> initSource(int sourceId,
	//			ArrayList<SemiDeserializedStreamRecord> inputList) {
	//		ArrayList<RecordId> outputList = new ArrayList<RecordId>();
	//
	//		for (SemiDeserializedStreamRecord record : inputList) {
	//			ftLayer.newSourceRecord(record, sourceId);
	//			outputList.add(record.getId());
	//		}
	//
	//		return outputList;
	//	}
	//
	//	private ArrayList<RecordId> xorSourceOutput(ArrayList<RecordId> inputList) {
	//		ArrayList<RecordId> outputList = new ArrayList<RecordId>();
	//
	//		for (RecordId oldId : inputList) {
	//			RecordId newId = RecordId.newRecordId(oldId.getSourceRecordId());
	//			ftLayer.xor(newId);
	//			outputList.add(newId);
	//			assertEquals(ftLayer.isFinished(), false);
	//		}
	//
	//		return outputList;
	//	}
	//
	//	private void xorTaskInput(ArrayList<RecordId> inputList) {
	//		ArrayList<RecordId> outputList = new ArrayList<RecordId>();
	//
	//		for (RecordId oldId : inputList) {
	//			ftLayer.xor(oldId);
	//			assertEquals(ftLayer.isFinished(), false);
	//		}
	//
	//	}
	//
	//	private void xorTaskInput(ArrayList<RecordId> inputList, ArrayList<Integer> failedInputs) {
	//		ArrayList<RecordId> outputList = new ArrayList<RecordId>();
	//		int counter = 0;
	//		for (RecordId oldId : inputList) {
	//			counter++;
	//			if (failedInputs.contains(counter)) {
	//				ftLayer.fail(oldId.getSourceRecordId());
	//			} else {
	//				ftLayer.xor(oldId);
	//				assertEquals(ftLayer.isFinished(), false);
	//			}
	//		}
	//
	//	}
	//
	//	private ArrayList<RecordId> xorTaskOutput(ArrayList<RecordId> inputList) {
	//		ArrayList<RecordId> outputList = new ArrayList<RecordId>();
	//
	//		for (RecordId oldId : inputList) {
	//			RecordId newId = RecordId.newRecordId(oldId.getSourceRecordId());
	//			ftLayer.xor(newId);
	//			outputList.add(newId);
	//			assertEquals(ftLayer.isFinished(), false);
	//		}
	//
	//		return outputList;
	//	}
	//
	//	private void xorSinkInput(ArrayList<RecordId> inputList) {
	//		for (RecordId oldId : inputList) {
	//			ftLayer.xor(oldId);
	//		}
	//	}
	//
	//	private ArrayList<Long> listSourceRecordIds(ArrayList<RecordId> recordIds) {
	//		ArrayList<Long> sourceRecordIds = new ArrayList<Long>();
	//		for (RecordId recordId : recordIds) {
	//			sourceRecordIds.add(recordId.getSourceRecordId());
	//		}
	//		Collections.sort(sourceRecordIds);
	//		return sourceRecordIds;
	//	}

	private void applyTask(int taskId) {
		ArrayList<Integer> downstreamTasks = topology.get(taskId);
		ArrayList<RecordId> inputList = inputLists.get(taskId);
		HashSet<Integer> failedRecordList = recordsToFail.get(taskId);
		Integer counter = counters.get(taskId);
		boolean lastTask = (taskId == executionOrder.get(executionOrder.size() - 1)) && !failed;

		for (RecordId oldId : inputList) {
			counters.put(taskId, ++counter);
			System.out.println("(" + taskId + "," + counter + ")");
			for (int task : downstreamTasks) {

				RecordId newId = RecordId.newRecordId(oldId.getSourceRecordId());
				ftLayer.xor(newId);
				inputLists.get(task).add(newId);
				assertEquals(ftLayer.isFinished(), false);

			}
			if (!sources.contains(taskId)) {
				if (failedRecordList.contains(counter)) {
					failedId = oldId.getSourceRecordId();
					failedObject = ftLayer.getSerializedObject(failedId);
					ftLayer.fail(failedId);
					failed = true;
					lastTask = false;
				} else {
					ftLayer.xor(oldId);
				}
				if (!lastTask) {
					assertEquals(ftLayer.isFinished(), false);
				}
			} else {
				if (failedRecordList.contains(counter)) {
					failedId = oldId.getSourceRecordId();
					failedObject = ftLayer.getSerializedObject(failedId);
					ftLayer.fail(failedId);
					failed = true;
					lastTask = false;
				}
			}
		}
		inputList.clear();
	}

	protected boolean isReady() {
		boolean ready = true;
		for (int task : tasks) {
			if (!sources.contains(task) && !inputLists.get(task).isEmpty()) {
				ready = false;
			}
		}
		return ready;
	}

	private void executeTopology() {
		initializeSourceInputs();
		do {
			for (int task : executionOrder) {
				if (!inputLists.get(task).isEmpty()) {
					applyTask(task);
					System.out.println("TaskApplied: " + task);
				}
			}
			inputLists = failedRecordLists;
			failedRecordLists = new HashMap<Integer, ArrayList<RecordId>>();
			for (int task : tasks) {
				failedRecordLists.put(task, new ArrayList<RecordId>());
			}
			failed = false;
		} while (!isReady());
		assertEquals(ftLayer.isFinished(), true);
	}

	private void getTasks() {
		tasks = new HashSet<Integer>();
		HashSet<Integer> nonSources = new HashSet<Integer>();
		HashSet<Integer> nonSinks = new HashSet<Integer>();

		for (int task : topology.keySet()) {
			tasks.add(task);
			nonSinks.add(task);
			tasks.addAll(topology.get(task));
			nonSources.addAll(topology.get(task));
		}
		sources = new HashSet<Integer>(tasks);
		sources.removeAll(nonSources);
		sinks = new HashSet<Integer>(tasks);
		sinks.removeAll(nonSinks);
		for (int sink : sinks) {
			topology.put(sink, new ArrayList<Integer>());
		}
	}

	private void initialize() {
		getTasks();
		initializeLists();
	}

	private void initializeLists() {
		ftLayer = new MockFTLayer(null);
		inputLists = new HashMap<Integer, ArrayList<RecordId>>();
		failedRecordLists = new HashMap<Integer, ArrayList<RecordId>>();
		recordsToFail = new HashMap<Integer, HashSet<Integer>>();
		counters = new HashMap<Integer, Integer>();
		for (int task : tasks) {
			inputLists.put(task, new ArrayList<RecordId>());
			failedRecordLists.put(task, new ArrayList<RecordId>());
			recordsToFail.put(task, new HashSet<Integer>());
			counters.put(task, 0);
		}
		sourceInputLists = new HashMap<Integer, ArrayList<SemiDeserializedStreamRecord>>();
		for (int task : sources) {
			sourceInputLists.put(task, new ArrayList<SemiDeserializedStreamRecord>());
		}
		failed = false;
	}

	private void initializeSourceInputs() {
		for (int source : sources) {
			for (SemiDeserializedStreamRecord record : sourceInputLists.get(source)) {
				ftLayer.newSourceRecord(record, source);
				inputLists.get(source).add(record.getId());
				assertEquals(ftLayer.isFinished(), false);
			}
		}
	}

	private void addEdge(int i, int j) {
		if (!topology.containsKey(i)) {
			topology.put(i, new ArrayList<Integer>());
		}
		topology.get(i).add(j);
	}

	private void createSourceInput(int source, byte from, byte to) {
		ArrayList<SemiDeserializedStreamRecord> sourceInputList = sourceInputLists.get(source);
		for (byte i = from; i < to + 1; i++) {
			sourceInputList.add(new SemiDeserializedStreamRecord(ByteBuffer.wrap(new byte[] { i }),
					0, RecordId.newSourceRecordId()));
		}
	}

	private void fail(int task, int inputNumber) {
		recordsToFail.get(task).add(inputNumber);
	}

	@Test
	public void ftLayerTestWithoutFail1() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// task-2 --> task-3;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(2, 3);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		executeTopology();

		//		ftLayer = new MockFTLayer(null);
		//		replayed = new HashMap<Integer, ArrayList<RecordId>>();
		//
		//		ArrayList<SemiDeserializedStreamRecord> sourceInputList1 = new ArrayList<SemiDeserializedStreamRecord>();
		//		ArrayList<Long> sourceRecordIdList1 = new ArrayList<Long>();
		//		for (byte i = 0; i < 3; i++) {
		//			sourceInputList1.add(new SemiDeserializedStreamRecord(
		//					ByteBuffer.wrap(new byte[] { i }), 0, RecordId.newSourceRecordId()));
		//		}
		//		ArrayList<RecordId> atSource1 = initSource(1, sourceInputList1);
		//		sourceRecordIdList1 = listSourceRecordIds(atSource1);
		//		Collections.sort(sourceRecordIdList1);
		//
		//		ArrayList<RecordId> fromSource1ToTask2 = xorSourceOutput(atSource1);
		//		assertEquals(listSourceRecordIds(fromSource1ToTask2), sourceRecordIdList1);
		//		ArrayList<RecordId> fromTask2ToTask3 = xorTaskOutput(fromSource1ToTask2);
		//		assertEquals(listSourceRecordIds(fromTask2ToTask3), sourceRecordIdList1);
		//		xorTaskInput(fromSource1ToTask2);
		//		ArrayList<RecordId> fromTask3ToSink4 = xorTaskOutput(fromTask2ToTask3);
		//		assertEquals(listSourceRecordIds(fromTask3ToSink4), sourceRecordIdList1);
		//		xorTaskInput(fromTask2ToTask3);
		//		xorSinkInput(fromTask3ToSink4);
		//
		//		assertEquals(ftLayer.isFinished(), true);

	}

	@Test
	public void ftLayerTestWithoutFail2() {
		// 0,1,2 --> source-1;
		// 3,4,5 --> source-2;

		// source-1 --> task-3;
		// source-2 --> task-3;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 3);
		addEdge(2, 3);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);
		createSourceInput(2, (byte) 0, (byte) 2);

		executeTopology();

		//		ftLayer = new MockFTLayer(null);
		//		replayed = new HashMap<Integer, ArrayList<RecordId>>();
		//
		//		ArrayList<SemiDeserializedStreamRecord> sourceInputList1 = new ArrayList<SemiDeserializedStreamRecord>();
		//		ArrayList<SemiDeserializedStreamRecord> sourceInputList2 = new ArrayList<SemiDeserializedStreamRecord>();
		//		ArrayList<Long> sourceRecordIdList1 = new ArrayList<Long>();
		//		ArrayList<Long> sourceRecordIdList2 = new ArrayList<Long>();
		//		for (byte i = 0; i < 3; i++) {
		//			sourceInputList1.add(new SemiDeserializedStreamRecord(
		//					ByteBuffer.wrap(new byte[] { i }), 0, RecordId.newSourceRecordId()));
		//		}
		//		for (byte i = 3; i < 6; i++) {
		//			sourceInputList2.add(new SemiDeserializedStreamRecord(
		//					ByteBuffer.wrap(new byte[] { i }), 0, RecordId.newSourceRecordId()));
		//		}
		//		ArrayList<RecordId> atSource1 = initSource(1, sourceInputList1);
		//		sourceRecordIdList1 = listSourceRecordIds(atSource1);
		//		ArrayList<RecordId> atSource2 = initSource(2, sourceInputList2);
		//		sourceRecordIdList2 = listSourceRecordIds(atSource2);
		//		Collections.sort(sourceRecordIdList1);
		//		Collections.sort(sourceRecordIdList2);
		//
		//		ArrayList<RecordId> fromSource1ToTask3 = xorSourceOutput(atSource1);
		//		assertEquals(listSourceRecordIds(fromSource1ToTask3), sourceRecordIdList1);
		//		ArrayList<RecordId> fromSource2ToTask3 = xorSourceOutput(atSource2);
		//		assertEquals(listSourceRecordIds(fromSource2ToTask3), sourceRecordIdList2);
		//		ArrayList<RecordId> fromTask3ToSink4 = xorTaskOutput(fromSource1ToTask3);
		//		assertEquals(listSourceRecordIds(fromTask3ToSink4), sourceRecordIdList1);
		//		ArrayList<RecordId> temp = xorTaskOutput(fromSource2ToTask3);
		//		assertEquals(listSourceRecordIds(temp), sourceRecordIdList2);
		//		fromTask3ToSink4.addAll(temp);
		//		xorTaskInput(fromSource1ToTask3);
		//		xorTaskInput(fromSource2ToTask3);
		//		xorSinkInput(fromTask3ToSink4);
		//
		//		assertEquals(ftLayer.isFinished(), true);

	}

	@Test
	public void ftLayerTestWithoutFail3() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// source-1 --> task-3;
		// task-2 --> sink-4;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(1, 3);
		addEdge(2, 4);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		executeTopology();

		//		ftLayer = new MockFTLayer(null);
		//		replayed = new HashMap<Integer, ArrayList<RecordId>>();
		//
		//		ArrayList<SemiDeserializedStreamRecord> sourceInputList1 = new ArrayList<SemiDeserializedStreamRecord>();
		//		ArrayList<Long> sourceRecordIdList1 = new ArrayList<Long>();
		//		for (byte i = 0; i < 3; i++) {
		//			sourceInputList1.add(new SemiDeserializedStreamRecord(
		//					ByteBuffer.wrap(new byte[] { i }), 0, RecordId.newSourceRecordId()));
		//		}
		//		ArrayList<RecordId> atSource1 = initSource(1, sourceInputList1);
		//		sourceRecordIdList1 = listSourceRecordIds(atSource1);
		//		Collections.sort(sourceRecordIdList1);
		//
		//		ArrayList<RecordId> fromSource1ToTask2 = xorSourceOutput(atSource1);
		//		assertEquals(listSourceRecordIds(fromSource1ToTask2), sourceRecordIdList1);
		//		ArrayList<RecordId> fromSource1ToTask3 = xorSourceOutput(atSource1);
		//		assertEquals(listSourceRecordIds(fromSource1ToTask3), sourceRecordIdList1);
		//		ArrayList<RecordId> fromTask2ToSink4 = xorTaskOutput(fromSource1ToTask2);
		//		assertEquals(listSourceRecordIds(fromTask2ToSink4), sourceRecordIdList1);
		//		xorTaskInput(fromSource1ToTask2);
		//		ArrayList<RecordId> fromTask3ToSink4 = xorTaskOutput(fromSource1ToTask3);
		//		assertEquals(listSourceRecordIds(fromTask3ToSink4), sourceRecordIdList1);
		//		xorTaskInput(fromSource1ToTask3);
		//		xorSinkInput(fromTask2ToSink4);
		//		xorSinkInput(fromTask3ToSink4);
		//
		//		assertEquals(ftLayer.isFinished(), true);

	}

	@Test
	public void ftLayerTestWithoutFail4() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// task-2 --> sink-3;
		// task-2 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(2, 3);
		addEdge(2, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		executeTopology();

		//		ftLayer = new MockFTLayer(null);
		//		replayed = new HashMap<Integer, ArrayList<RecordId>>();
		//
		//		ArrayList<SemiDeserializedStreamRecord> sourceInputList1 = new ArrayList<SemiDeserializedStreamRecord>();
		//		ArrayList<Long> sourceRecordIdList1 = new ArrayList<Long>();
		//		for (byte i = 0; i < 3; i++) {
		//			sourceInputList1.add(new SemiDeserializedStreamRecord(
		//					ByteBuffer.wrap(new byte[] { i }), 0, RecordId.newSourceRecordId()));
		//		}
		//		ArrayList<RecordId> atSource1 = initSource(1, sourceInputList1);
		//		sourceRecordIdList1 = listSourceRecordIds(atSource1);
		//		Collections.sort(sourceRecordIdList1);
		//
		//		ArrayList<RecordId> fromSource1ToTask2 = xorSourceOutput(atSource1);
		//		assertEquals(listSourceRecordIds(fromSource1ToTask2), sourceRecordIdList1);
		//		ArrayList<RecordId> fromTask2ToSink3 = xorTaskOutput(fromSource1ToTask2);
		//		assertEquals(listSourceRecordIds(fromTask2ToSink3), sourceRecordIdList1);
		//		ArrayList<RecordId> fromTask2ToSink4 = xorTaskOutput(fromSource1ToTask2);
		//		assertEquals(listSourceRecordIds(fromTask2ToSink4), sourceRecordIdList1);
		//		xorTaskInput(fromSource1ToTask2);
		//		xorSinkInput(fromTask2ToSink3);
		//		xorSinkInput(fromTask2ToSink4);
		//
		//		assertEquals(ftLayer.isFinished(), true);

	}

	@Test
	public void ftLayerTestWithFail1() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// task-2 --> task-3;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(2, 3);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		fail(2, 1);
		fail(2, 2);
		fail(4, 3);

		executeTopology();
	}

	@Test
	public void ftLayerTestWithFail2() {
		// 0,1,2 --> source-1;
		// 3,4,5 --> source-2;

		// source-1 --> task-3;
		// source-2 --> task-3;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 3);
		addEdge(2, 3);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);
		createSourceInput(2, (byte) 0, (byte) 2);

		fail(1, 3);
		fail(3, 3);

		executeTopology();
	}

	@Test
	public void ftLayerTestWithFail3() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// source-1 --> task-3;
		// task-2 --> sink-4;
		// task-3 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(1, 3);
		addEdge(2, 4);
		addEdge(3, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		fail(1, 2);
		fail(2, 1);

		executeTopology();
	}

	@Test
	public void ftLayerTestWithFail4() {
		// 0,1,2 --> source-1;

		// source-1 --> task-2;
		// task-2 --> sink-3;
		// task-2 --> sink-4;

		topology = new HashMap<Integer, ArrayList<Integer>>();
		addEdge(1, 2);
		addEdge(2, 3);
		addEdge(2, 4);

		executionOrder = Arrays.asList(1, 2, 3, 4);

		initialize();

		createSourceInput(1, (byte) 0, (byte) 2);

		fail(1, 3);
		fail(1, 2);

		executeTopology();
	}

	public class MyThread extends Thread {
		protected Thread t;
		protected ArrayList<Integer> inputList;
		protected MyThread next;
		protected boolean finished;
		protected long waitingTime;
		protected long processingTime;
		protected Random rnd;
		protected CyclicBarrier gate;

		public MyThread(ArrayList<Integer> inputList, MyThread next, long waitingTime,
				long processingTime, CyclicBarrier gate) {
			this.t = new Thread(this);
			this.inputList = inputList;
			this.next = next;
			this.finished = false;
			this.waitingTime = waitingTime;
			this.processingTime = processingTime;
			this.rnd = new Random();
			this.gate = gate;
		}

		protected void doubleWaitingTime() {
			waitingTime *= 2;
			System.out.println("Waiting time doubled");
		}

		protected void resetWaitingTime() {
			waitingTime = 10;
		}

		public void start() {
			t.start();
		}

	}

}
