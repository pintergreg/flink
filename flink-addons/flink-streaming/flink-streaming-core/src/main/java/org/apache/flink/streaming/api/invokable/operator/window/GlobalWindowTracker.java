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

package org.apache.flink.streaming.api.invokable.operator.window;

import java.util.LinkedList;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class GlobalWindowTracker<IN> extends
		StreamInvokable<IN, Tuple4<IN, Integer, Integer, Boolean>> {

	private static final long serialVersionUID = -8232375561456225043L;

	private LinkedList<TriggerPolicy<IN>> triggerPolicies;
	private LinkedList<EvictionPolicy<IN>> evictionPolicies;
	private LinkedList<ActiveTriggerPolicy<IN>> activeTriggerPolicies;
	private LinkedList<ActiveEvictionPolicy<IN>> activeEvictionPolicies;
	private LinkedList<Thread> activePolicyThreads;

	private int currentBufferSize = 0;
	private final int numberOfPreAggregators;
	private int nextChannelToEmit = 0;
	private int firstInRound = 0;
	private int currentWindowId = 0;

	private Tuple4<IN, Integer, Integer, Boolean> output = new Tuple4<IN, Integer, Integer, Boolean>();

	public GlobalWindowTracker(LinkedList<TriggerPolicy<IN>> triggerPolicies,
			LinkedList<EvictionPolicy<IN>> evictionPolicies, int numberOfPreAggregators) {
		super(null);

		this.triggerPolicies = triggerPolicies;
		this.evictionPolicies = evictionPolicies;
		this.numberOfPreAggregators = numberOfPreAggregators;

		activeTriggerPolicies = new LinkedList<ActiveTriggerPolicy<IN>>();
		for (TriggerPolicy<IN> tp : triggerPolicies) {
			if (tp instanceof ActiveTriggerPolicy) {
				activeTriggerPolicies.add((ActiveTriggerPolicy<IN>) tp);
			}
		}

		activeEvictionPolicies = new LinkedList<ActiveEvictionPolicy<IN>>();
		for (EvictionPolicy<IN> ep : evictionPolicies) {
			if (ep instanceof ActiveEvictionPolicy) {
				activeEvictionPolicies.add((ActiveEvictionPolicy<IN>) ep);
			}
		}

		this.activePolicyThreads = new LinkedList<Thread>();
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);
		for (ActiveTriggerPolicy<IN> tp : activeTriggerPolicies) {
			Runnable target = tp.createActiveTriggerRunnable(new WindowingCallback(tp));
			if (target != null) {
				Thread thread = new Thread(target);
				activePolicyThreads.add(thread);
				thread.start();
			}
		}
	};

	/**
	 * This class allows the active trigger threads to call back and push fake
	 * elements at any time.
	 */
	private class WindowingCallback implements ActiveTriggerCallback {
		private ActiveTriggerPolicy<IN> policy;

		public WindowingCallback(ActiveTriggerPolicy<IN> policy) {
			this.policy = policy;
		}

		@Override
		public void sendFakeElement(Object datapoint) {
			processFakeElement(datapoint, this.policy);
		}
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			// Prevent empty data streams
			if (nextRecord == null) {
				throw new RuntimeException("DataStream must not be empty");
			}

			// Continuously run
			while (nextRecord != null) {
				processRealElement(nextObject);

				// Load next StreamRecord
				readNext();
			}

			// Stop all remaining threads from policies
			for (Thread t : activePolicyThreads) {
				t.interrupt();
			}

			// finally trigger the buffer.
			emitFinalWindow();
		}
	}

	/**
	 * 
	 * 
	 * @param input
	 *            a fake input element
	 * @param currentPolicy
	 *            the policy which produced this fake element
	 */
	protected synchronized void processFakeElement(Object input, TriggerPolicy<IN> currentPolicy) {

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (ActiveEvictionPolicy<IN> evictionPolicy : activeEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEvictionWithFakeElement(input, currentBufferSize);
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		submit(null, currentMaxEviction, false);
		submit(null, 0, true);
	}

	/**
	 * 
	 * 
	 * @param input
	 *            a real input element
	 */
	protected synchronized void processRealElement(IN input) {

		// Run the precalls to detect missed windows
		for (ActiveTriggerPolicy<IN> trigger : activeTriggerPolicies) {
			// Remark: In case multiple active triggers are present the ordering
			// of the different fake elements returned by this triggers becomes
			// a problem. This might lead to unexpected results...
			// Should we limit the number of active triggers to 0 or 1?
			Object[] result = trigger.preNotifyTrigger(input);
			for (Object in : result) {
				processFakeElement(in, trigger);
			}
		}

		// Remember if a trigger occurred
		boolean isTriggered = false;

		// Process the triggers
		for (TriggerPolicy<IN> triggerPolicy : triggerPolicies) {
			if (triggerPolicy.notifyTrigger(input)) {
				// remember trigger
				isTriggered = true;
			}
		}

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;

		for (EvictionPolicy<IN> evictionPolicy : evictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEviction(input, isTriggered, currentBufferSize);
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		submit(input, currentMaxEviction, isTriggered);

	}

	protected void emitFinalWindow() {
		submit(null, 0, true);
	}

	protected void submit(IN element, int numToEvict, boolean trigger) {

		if (element != null && numToEvict == 0 && trigger == false) {
			// this is the simple case: only forward the real element

			output.f0 = element;
			output.f1 = currentWindowId;
			output.f2 = -1;
			output.f3 = true;

			collector.collect(output);

			nextChannelToEmit = (nextChannelToEmit + 1) % numberOfPreAggregators;

		} else {
			if (trigger) {
				output.f1 = ++currentWindowId;
			}
			int baseEvict = numToEvict / numberOfPreAggregators;
			int additionallyEvict = numToEvict % numberOfPreAggregators;

			for (int i = 0; i < numberOfPreAggregators; i++) {

				output.f2 = baseEvict;

				// Add element
				if (i == 0 && element != null) {
					output.f0 = element;
					output.f3 = true;
				} else {
					output.f3 = false;
				}

				boolean cond1 = nextChannelToEmit < firstInRound + additionallyEvict
						&& nextChannelToEmit >= firstInRound;
				boolean cond2 = nextChannelToEmit < (firstInRound + additionallyEvict)
						% numberOfPreAggregators;

				if (additionallyEvict > 0 && (cond1 || cond2)) {
					output.f2++;
				}

				if (output.f3 || trigger || output.f2 > 0) {
					collector.collect(output);
					nextChannelToEmit = (nextChannelToEmit + 1) % numberOfPreAggregators;

				}
			}

			firstInRound = nextChannelToEmit;
		}

		// Remember the current size of the buffer
		if (currentBufferSize - numToEvict < 0) {
			currentBufferSize = 0;
		} else {
			currentBufferSize -= numToEvict;
		}

		// I there is a new element increase the buffer
		if (element != null) {
			currentBufferSize++;
		}
	}
}
