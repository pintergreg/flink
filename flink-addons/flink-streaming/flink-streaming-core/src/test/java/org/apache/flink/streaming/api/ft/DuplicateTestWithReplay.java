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

package org.apache.flink.streaming.api.ft;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.FTLayerBuilder;
import org.apache.flink.streaming.api.NOpFTLayerBuilder;
import org.apache.flink.streaming.api.OpFTLayerBuilder;
import org.apache.flink.streaming.api.StreamGraph;
import org.apache.flink.streaming.api.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.event.FailException;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.id.RecordWithHashCode;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerConfig;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerVertex;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Test created for testing edge information gathering and replaypartition setting
 */
public class DuplicateTestWithReplay {

	public static void main(String[] args) throws Exception {

		numberSequenceWithoutShuffle();
	}

	/*
	 * ACTUAL TEST-TOPOLOGY METHODS
	 */


	private static void numberSequenceWithoutShuffle() throws Exception {
		// set up the execution environment
		//final StreamExecutionEnvironment env = MockTestStreamEnvironment.getExecutionEnvironment();
		final StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();

		// building the job graph
		/*
		*  (So)--(M)--(Si)
		*
		* Source emits numbers as String from 0 to 9
		* Filter does nothing, lets pass everything
		* Sink prints values to standard error output
		*/
		DataStream<Integer> sourceStream1 = env.addSource(new NumberSource(10)).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);
		sourceStream1.map(new NumberMap()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER).addSink(new SimpleSink()).setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);


		//run this topology
		env.execute();
	}


	/*
	 * SOURCE CLASSES
	 */

	private static final class NumberSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private int n;
		Type type;

		public enum Type {
			ODD, EVEN, BOTH
		}

		public NumberSource(int n) {
			this.n = n;
			this.type = Type.BOTH;
		}

		public NumberSource(int n, Type type) {
			this.n = n;
			this.type = type;
		}

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {
			int step;
			int start;
			switch (this.type) {
				case EVEN:
					step = 2;
					start = 0;
					break;
				case ODD:
					step = 2;
					start = 1;
					break;
				case BOTH:
				default:
					step = 1;
					start = 0;
					break;
			}
			for (int i = start; i < n; i += step) {
				Thread.sleep(105L);
				collector.collect(i);
			}

		}
	}

	/*
	 * MAP CLASSES
	 */

	public static class NumberMap implements MapFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		public NumberMap() {
		}

		@Override
		public String map(Integer value) throws Exception {
			return value.toString();
		}
	}

	public static class FailMap implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		private ArrayList<String> recordsToFail;

		public FailMap() {
			this.recordsToFail = new ArrayList<String>();
		}

		@Override
		public String map(String value) throws Exception {
			//testFail(value, recordsToFail);
			return value.toString();
		}

		// fails a source record id if it is the first time to be seen
		private void testFail(String value, ArrayList<String> recordsToFail) throws FailException, InterruptedException {
			if (!recordsToFail.contains(value)) {
				recordsToFail.add(value);

				System.out.println(recordsToFail);
				throw new FailException();
			}

		}
	}

	/*
	 * SINK CLASSES
	 */

	public static class SimpleSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		public SimpleSink() {

		}

		@Override
		public void invoke(String value) {
			System.err.println(value);
		}
	}

	/*
	 * MOCK ENVIRONMENT FOR TESTING
	 */

	// list of failed record ids
	private static ArrayList<Long> failedIds = new ArrayList<Long>();
	private static ArrayList<Long> recordIds = new ArrayList<Long>();

	public static class MockTestStreamEnvironment extends TestStreamEnvironment {

		public MockTestStreamEnvironment(int degreeOfParallelism, long memorySize) {
			super(degreeOfParallelism, memorySize);
			this.streamGraph = new MockStreamGraph();
		}
	}

	private static class MockStreamGraph extends StreamGraph {

		@Override
		public JobGraph getJobGraph(String jobGraphName) {

			this.jobName = jobGraphName;
			StreamingJobGraphGenerator optimizer = new MockStreamingJobGraphGenerator(this);

			return optimizer.createJobGraph(jobGraphName);
		}

	}

	private static class MockStreamingJobGraphGenerator extends StreamingJobGraphGenerator {

		public MockStreamingJobGraphGenerator(StreamGraph streamGraph) {
			super(streamGraph);
		}

		private void init() {
			if (ftStatus == FTLayerBuilder.FTStatus.ON) {
				this.ftBuilder = new MockOpFTLayerBuilder(this);
			} else {
				this.ftBuilder = new NOpFTLayerBuilder();
			}
		}
	}

	private static class MockOpFTLayerBuilder extends OpFTLayerBuilder {
		private static final Logger LOG = LoggerFactory.getLogger(MockOpFTLayerBuilder.class);

		public MockOpFTLayerBuilder(StreamingJobGraphGenerator jobGraphGenerator) {
			super(jobGraphGenerator);
		}

		@Override
		public void createFTLayerVertex(JobGraph jobGraph, int parallelism) {
			String vertexName = "MockFTLayerVertex";
			Class<? extends AbstractInvokable> vertexClass = MockFTLayerVertex.class;
			ftLayerVertex = new AbstractJobVertex(vertexName);
			jobGraph.addVertex(ftLayerVertex);
			ftLayerVertex.setInvokableClass(vertexClass);
			ftLayerVertex.setParallelism(parallelism);
			if (LOG.isDebugEnabled()) {
				LOG.debug("FTLayer parallelism set: {} for {}", parallelism, vertexName);
			}
			FTLayerConfig config = new FTLayerConfig(ftLayerVertex.getConfiguration());
			config.setNumberOfSources(sourceVertices.size());
			config.setBufferTimeout(100L);
		}
	}


	public static class MockFTLayerVertex extends FTLayerVertex {

		public MockFTLayerVertex() {
			instanceID = numOfTasks;
			ftLayer = new MockFTLayer();
			numOfTasks++;
		}

	}

	// FTLayer is mocked in order to collect xor messages and failed ids into lists
	public static class MockFTLayer extends FTLayer {
		private final static Logger LOG = LoggerFactory.getLogger(MockFTLayer.class);

		public MockFTLayer() {
			super();
		}

		@Override
		public void explicitFail(long sourceRecordId) {

				failedIds.add(sourceRecordId);
				failedSourceRecordIds.add(sourceRecordId);

				// find sourceRecord
				int sourceId = sourceIdOfRecord.get(sourceRecordId);
				RecordWithHashCode recordWithHashCode = persistenceLayer.get(sourceRecordId);

				// generate new sourceRecordId with the deterministic ID generation
				RecordId newSourceRecordId = RecordId.newReplayedRootId(sourceRecordId);

				long newId = newSourceRecordId.getSourceRecordId();

				// add new sourceRecord to PersistenceLayer
				sourceIdOfRecord.put(newId, sourceId);
				persistenceLayer.put(newId, recordWithHashCode);



				// delete sourceRecord from PersistenceLayer
				sourceIdOfRecord.remove(sourceRecordId);
				persistenceLayer.remove(sourceRecordId);



				// create new sourceRecord
				SemiDeserializedStreamRecord newSourceRecord = new SemiDeserializedStreamRecord(
						ByteBuffer.wrap(recordWithHashCode.getSerializedRecord()),
						recordWithHashCode.getHashCode(), newSourceRecordId);

				// send new sourceRecord to sourceSuccessives
				collectToSourceSuccessiveTasks(sourceId, newSourceRecord);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Source record id {} has been failed", Long.toHexString(sourceRecordId));
					LOG.debug("Failed source record id {} is replaced by {}",
							Long.toHexString(sourceRecordId), Long.toHexString(newId));
				}

		}

		@Override
		public void newSourceRecord(SemiDeserializedStreamRecord sourceRecord, int sourceId) {
			long sourceRecordId = sourceRecord.getId().getSourceRecordId();
			int hashCode = sourceRecord.getHashCode();
			byte[] serializedRecord = sourceRecord.getArray();
			RecordWithHashCode recordWithHashCode = new RecordWithHashCode(serializedRecord,
					hashCode);
			if (!ackerTable.contains(sourceRecordId)) {
				ackerTable.newSourceRecord(sourceRecordId);
			}
			sourceIdOfRecord.put(sourceRecordId, sourceId);
			persistenceLayer.put(sourceRecordId, recordWithHashCode);
		}

		@Override
		public void xor(RecordId recordId) {

			long sourceRecordId = recordId.getSourceRecordId();
			if (!failedSourceRecordIds.contains(sourceRecordId)) {
				if (!ackerTable.contains(sourceRecordId)) {
					ackerTable.newSourceRecord(sourceRecordId);
				}
//				HashMap<Long, Integer> recordIdMap = xorMessagesReceived.get(sourceRecordId);
//				if (recordIdMap.containsKey(recordId.getCurrentRecordId())) {
//					int counter = recordIdMap.get(recordId.getCurrentRecordId());
//					recordIdMap.put(recordId.getCurrentRecordId(), ++counter);
//
//				} else {
//					recordIdMap.put(recordId.getCurrentRecordId(), 1);
//				}
				ackerTable.xor(sourceRecordId, recordId.getCurrentRecordId());
			}
		}

		@Override
		protected void collectToSourceSuccessiveTasks(int sourceId,
				SemiDeserializedStreamRecord sourceRecord) {
			super.collectToSourceSuccessiveTasks(sourceId, sourceRecord);
		}

	}

	public class MockStreamInvokable extends StreamInvokable{

		public MockStreamInvokable(Function userFunction) {
			super(userFunction);
		}

		@Override
		public void invoke() throws Exception {

		}

		@Override
		protected void callUserFunctionAndLogException() {
			try {
				abstractFTHandler.setAnchorRecord(nextRecord);
				callUserFunction();
				ackAnchorRecord();
			} catch (FailException e) {

			} catch (Exception e) {
				System.out.println("körte");
			}
		}
	}
}
