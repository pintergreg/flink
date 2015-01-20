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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class GlobalWindowTracker<IN> extends StreamInvokable<IN, Tuple3<IN, Integer, Integer>> {

	private static final long serialVersionUID = -8232375561456225043L;

	private LinkedList<TriggerPolicy<IN>> triggerPolicies;
	private LinkedList<EvictionPolicy<IN>> evictionPolicies;
	private LinkedList<ActiveTriggerPolicy<IN>> activeTriggerPolicies;
	private LinkedList<ActiveEvictionPolicy<IN>> activeEvictionPolicies;
	private LinkedList<Thread> activePolicyThreads;
	private LinkedList<TriggerPolicy<IN>> currentTriggerPolicies;

	private Tuple3<IN, Integer, Integer> output = new Tuple3<IN, Integer, Integer>();

	public GlobalWindowTracker(LinkedList<TriggerPolicy<IN>> triggerPolicies,
			LinkedList<EvictionPolicy<IN>> evictionPolicies) {
		super(null);

		this.triggerPolicies = triggerPolicies;
		this.evictionPolicies = evictionPolicies;

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
		this.currentTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			// TODO:Implement this
		}
	}

}
