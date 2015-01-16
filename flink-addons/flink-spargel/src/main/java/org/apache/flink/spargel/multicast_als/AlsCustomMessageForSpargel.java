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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class AlsCustomMessageForSpargel implements Serializable, IOReadableWritable {
	
	private static final long serialVersionUID = 1L;
	private int id; //sender id
	private double[] data;//column of the sender

	public AlsCustomMessageForSpargel() {
		this.id = -1;
		this.data = null;
	}

	public AlsCustomMessageForSpargel(int id, double[] data) {
		this.id = id;
		this.data = data;
	}
	
	public AlsCustomMessageForSpargel(AlsCustomMessageForSpargel other) {
		this(other.id,other.data);
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setData(double[] data) {
		this.data = data;
	}

	public int getId() {
		return id;
	}

	public double[] getData() {
		return data;
	}

	public int getSize() {
		return (this.data == null ? 0 : this.data.length);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.id);
		out.writeInt(getSize());
		for (int i = 0; i < getSize(); i++) {
			out.writeDouble(this.data[i]);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.id = in.readInt();
		int dataSize = in.readInt();
		if (dataSize > 0) {
			this.data = new double[dataSize];
			for (int i = 0; i < dataSize; i++) {
				this.data[i] = in.readDouble();
			}
		} else {
			this.data = null;
		}
	}

	@Override
	public String toString() {
		return "CustomMessage [id=" + id + ", data=" + Arrays.toString(data)
				+ "]";
	}

}
