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
package org.apache.flink.spargel.multicast_als;

public class IdFormatter {

	public static int getVertexId(boolean partOfQ, int originalId) {
		return (partOfQ ? 2 * originalId : 2 * originalId + 1);
	}

	public static int getOriginalId(int vertexId) {
		return (vertexId % 2 == 0 ? vertexId / 2 : (vertexId - 1) / 2);
	}

	public static int getMultiVertexIdFromVertexId(int vertexId,
			int numOfSubTasks) {
		boolean partOfQ = (vertexId % 2 == 0);
		if (partOfQ) {
			// return ((vertexId / 2) % numOfSubTasks) * 2;
			return (getOriginalId(vertexId) % numOfSubTasks) * 2;
		} else {
			// return (((vertexId - 1) / 2) % numOfSubTasks) * 2 + 1;
			return (getOriginalId(vertexId) % numOfSubTasks) * 2 + 1;
		}
	}
}
