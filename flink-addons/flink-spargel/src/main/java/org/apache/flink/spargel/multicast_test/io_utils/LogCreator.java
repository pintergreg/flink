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
package org.apache.flink.spargel.multicast_test.io_utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

public class LogCreator {
	private String outputPath;
	private PrintWriter pw;
	private Date startTime;

	private final DateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public LogCreator(String filePath) {
		outputPath = filePath;
		startTime = new Date();
	}

	public void writeAlsParameters(String algorithm, long timeTaken,
			String input, String output, int numTasks, int numIterations,
			int k, double lambda, String whichSolver, int whichMulticast) {

		try {
			pw = new PrintWriter(new FileWriter(new File(outputPath), true));
			pw.println("#Parameters of the als job:");

			pw.println("start time: " + dateFormat.format(this.startTime));
			pw.println("input file: " + input);
			pw.println("output file: " + output);
			pw.println("numOfTasks: " + numTasks);
			pw.println("k: " + k);
			pw.println("lambda: " + Double.toString(lambda));
			pw.println("iter: " + numIterations);
			pw.println("solver: " + whichSolver);
			pw.println("program: " + algorithm);
			pw.println("multicast_version: " + whichMulticast);
			pw.println("Time taken: " + Integer.toString((int) timeTaken));

		} catch (IOException io) {
			io.printStackTrace();
		}
	}

	public void writePageRankParameters(String algorithm, long timeTaken,
			String input, String output, int numTasks, int numberOfPartitions,
			int maxIterations, double epsilon, double teleport) {

		try {
			pw = new PrintWriter(new FileWriter(new File(outputPath), true));
			pw.println("#Parameters of the pagerank job:");

			pw.println("start time: " + dateFormat.format(this.startTime));
			pw.println("input file: " + input);
			pw.println("output file: " + output);
			pw.println("numOfTasks: " + numTasks);
			pw.println("numberOfPartitions: " + (algorithm.matches(".*Custom.*") ? numberOfPartitions : 0));
			pw.println("epsilon: " + epsilon);
			pw.println("teleport: " + Double.toString(teleport));
			pw.println("iter: " + maxIterations);
			pw.println("program: " + algorithm);
			pw.println("Time taken: " + Integer.toString((int) timeTaken));

		} catch (IOException io) {
			io.printStackTrace();
		}
	}

	public void close() {
		pw.close();
	}

}
