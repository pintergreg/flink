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

package org.apache.flink.streaming.api.invokable;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.ExactlyOnceParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pintergreg.bloomfilter.A2BloomFilter;

import java.util.HashSet;

public class SinkInvokable<IN> extends ChainableInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SinkInvokable.class);

	private SinkFunction<IN> sinkFunction;

	private HashSet<Long> idStore=new HashSet<Long>();
	int counter=0;
	long time;
	private A2BloomFilter bloomFilter;
	private boolean isExactlyOnce = false;

	public SinkInvokable(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		this.sinkFunction = sinkFunction;
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		/*
		 * Is this record seen before?
		 */
//		if (this.isExactlyOnce) {
//			if (!idStore.contains(nextRecord.getId().getCurrentRecordId())) {
//				idStore.add(nextRecord.getId().getCurrentRecordId());
//
//				sinkFunction.invoke(nextObject);
//				counter++;
//				//System.out.println("\t\t\t\t" + nextRecord.getId().getCurrentRecordId() + "\tcontent:" + (String) nextObject);
//			} else {
//				//System.out.println("\tI'VE ALREADY SEEN THIS BEFORE: "+nextRecord.getId().getCurrentRecordId()+"\tcontent:"+(String)nextObject);
//			}
//		}else{
//			counter++;
//			sinkFunction.invoke(nextObject);
//		}

		if (this.isExactlyOnce) {
			if (!bloomFilter.include(nextRecord.getId().getCurrentRecordId())) {
				bloomFilter.add(nextRecord.getId().getCurrentRecordId());

				sinkFunction.invoke(nextObject);
				counter++;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Bloom Filter has not seen this before with the ID of {}, and the content of: {}", nextRecord.getId().getCurrentRecordId(), String.valueOf(nextObject));
				}
			} else if (LOG.isDebugEnabled()) {
				LOG.debug("BLOOMFILTER HAS ALREADY SEEN THIS BEFORE WITH THE ID OF {}, AND THE CONTENT OF: {}", nextRecord.getId().getCurrentRecordId(), String.valueOf(nextObject));
			}

		} else {
			sinkFunction.invoke(nextObject);
			counter++;
		}

	}

	@Override
	public void collect(IN record) {
		nextObject = copy(record);
		callUserFunctionAndLogException();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		isRunning = true;
		this.isExactlyOnce = taskContext.getConfig().getExactlyOnce();
		if (this.isExactlyOnce) {
			ExactlyOnceParameters p = taskContext.getConfig().getExactlyOnceParameters();
			this.bloomFilter = new A2BloomFilter(p.getN(), p.getP(), p.getTtl());
		}
		FunctionUtils.openFunction(userFunction, parameters);
		time=System.nanoTime();
	}

	@Override
	public void close() {
		isRunning = false;
		collector.close();
		try {
			FunctionUtils.closeFunction(userFunction);
		} catch (Exception e) {
			throw new RuntimeException("Error when closing the function: " + e.getMessage());
		} finally {
			if (this.isExactlyOnce) {
				this.bloomFilter.stopTimer();
			}
			System.err.println(String.valueOf(counter)+" in:"+ String.valueOf(System.nanoTime()-time));
		}
	}

}
