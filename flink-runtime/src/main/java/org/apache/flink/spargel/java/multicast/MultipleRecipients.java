/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.spargel.java.multicast;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultipleRecipients<VertexKey extends Comparable<VertexKey>>
		implements Iterable<VertexKey> {

	@Override
	public String toString() {
		return "MultipleRecipients [recipients=" + recipients + "]";
	}


	private List<VertexKey> recipients;

	public MultipleRecipients() {
		recipients = new ArrayList<VertexKey>();
	}

	public void addRecipient(VertexKey recipient) {
		recipients.add(recipient);
	}
	
	public int getNumberOfRecipients(){
		return recipients.size();
	}
	
	
	// Implementing the iterable and the iterator interface

	@Override
	public Iterator<VertexKey> iterator() {
		return recipients.iterator();
	}

}
