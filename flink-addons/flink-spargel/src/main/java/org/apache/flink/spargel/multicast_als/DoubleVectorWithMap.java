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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class DoubleVectorWithMap implements Serializable{
	private static final long serialVersionUID = 1L;

	private int id; // vertex id
	private double[] data = new double[0];// the double vector of p or q
	private HashMap<String, Double> edges = new HashMap<String, Double>();

	public DoubleVectorWithMap() {
	}

	public DoubleVectorWithMap(int id, double[] data) {
		this.id = id;
		this.data = data;
	}

	public DoubleVectorWithMap(int id, double[] data,
			HashMap<String, Double> edges) {
		this.id = id;
		this.data = data;
		this.edges = new HashMap<String, Double>(edges);
	}

	public DoubleVectorWithMap(DoubleVectorWithMap object) {
		this(object.getId(), object.getData(), object.getEdges());
	}

	public void clearEdges() {
		this.edges.clear();
	}

	public int getId() {
		return id;
	}

	public double[] getData() {
		return data;
	}

	public HashMap<String, Double> getEdges() {
		return edges;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setData(double[] data) {
		this.data = data;
	}

	public void setEdges(HashMap<String, Double> edges) {
		this.edges = edges;
	}

	@Override
	public String toString() {
		return "DoubleVectorWithMap2 [id=" + id + ", data=" + dataToStr()
				+ ", edges=" + edgesToStr() + "]";
	}

	public String dataToStr() {
		String out = "";
		for (double i : data) {
			out += i + ",";
		}
		return out;
	}

	public String edgesToStr() {
		String out = "";
		for (String i : edges.keySet()) {
			out += "[" + i + "," + edges.get(i) + "],";
		}
		return out;
	}

	public Set<String> getMapKeySet() {
		return edges.keySet();
	}

	public double getEdgeValue(int key) {
		return this.edges.get(Integer.toString(key));
	}
}
