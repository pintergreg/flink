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
package org.apache.flink.spargel.multicast_runners;

import org.apache.flink.spargel.multicast_als.SpargelAls;
import org.apache.flink.spargel.multicast_test.io_utils.LinalgSolver;
import org.apache.flink.spargel.multicast_test.io_utils.LogCreator;

public class AlsRunner {

	private static String[] programs = { "AlsWithMap" };

	public static void main(String[] args) throws Exception {
		int numOfParameters = 9;

		if (args.length < numOfParameters) {
			System.out
					.println("Parameters: [whichProgram] [whichMulticast] [noSubStasks] [matrix] [output] [rank] [lambda] [numberOfIterations] [whichSolver] [LogPath:OPTIONAL]");
		} else {
			// parse parameters:
			String whichProgram = args[0];
			int whichMulticast = Integer.parseInt(args[1]);
			int numTasks = Integer.parseInt(args[2]);
			String matrixSource = args[3];
			String outputPath = args[4];
			int k = Integer.parseInt(args[5]);
			double lambda = Double.parseDouble(args[6]);
			int numIterations = Integer.parseInt(args[7]);
			String whichSolver = args[8];

			boolean enableLogging = false;
			LogCreator logger = null;
			String logPath = "";
			boolean legalParameters = true;
			long startTime = 0L;
			long endTime = 0L;
			long totalTime = 0L;

			if (args.length > numOfParameters) {
				enableLogging = true;
				logPath = args[numOfParameters];
			}

			try {
				// start logging
				if (enableLogging) {
					logger = new LogCreator(logPath + "/log");
					startTime = System.currentTimeMillis();
				}
				
				if (LinalgSolver.isLegalSolver(whichSolver)) {

					if (whichProgram.equals("AlsWithMap")) {
						new SpargelAls().runAls(numTasks, matrixSource,
								outputPath, k, lambda, numIterations,
								whichSolver, whichMulticast);
					} else {
						legalParameters = false;
						throw new IllegalArgumentException(
								printOptions(whichProgram));
					}
				} else {
					throw new IllegalArgumentException(
							LinalgSolver.printOptions(whichSolver));
				}

				// finish logging
				if (enableLogging) {
					endTime = System.currentTimeMillis();
					totalTime = endTime - startTime;
					if (legalParameters) {
						logger.writeAlsParameters(whichProgram, totalTime,
								matrixSource, outputPath, numTasks,
								numIterations, k, lambda, whichSolver,
								whichMulticast);
					}
				}
			} catch (IllegalArgumentException iaex) {
				iaex.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				if (logger != null) {
					logger.close();
				}
			}
		}
	}

	public static String printOptions(String whichProgram) {
		String errorMsg = whichProgram
				+ " program does not exist! Available programs: ";
		int index = 0;
		for (String i : programs) {
			++index;
			errorMsg += " " + i;
			errorMsg += (index == programs.length ? "." : ",");
		}
		return errorMsg;
	}
}
