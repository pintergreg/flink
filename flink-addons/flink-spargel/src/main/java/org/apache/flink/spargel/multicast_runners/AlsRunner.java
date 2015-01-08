package org.apache.flink.spargel.multicast_runners;

import org.apache.flink.spargel.multicast_als.SpargelAls;
import org.apache.flink.spargel.multicast_test.io_utils.LinalgSolver;
import org.apache.flink.spargel.multicast_test.io_utils.LogCreator;

public class AlsRunner {

	private static String[] programs = {"AlsWithMap"};

	public static void main(String[] args) throws Exception {
		int numOfParameters = 9;
//
//		if (args.length < numOfParameters) {
//			System.out
//					.println("Parameters: [WhichProgram] [noSubStasks] [matrix] [output] [rank] [lambda] [numberOfIterations] [qSource:'-' if random q] [whichSolver] [LogPath:OPTIONAL]");
//		} else {

			// manual parameters:
			String whichProgram = "AlsWithMap";
			int numTasks = 1;
			String matrixSource = "/home/fberes/sztaki/git/cumulo-strato/data/sampledb2b.csv.txt";
			String outputPath = "/home/fberes/sztaki/git/incubator-flink-output/test";
			int k = 5;
			double lambda = 0.01;
			int numIterations = 3;
			String qSource = "-";
			String whichSolver = "jama";
			
//			// parse parameters:
//			String whichProgram = args[0];
//			int numTasks = Integer.parseInt(args[1]);
//			String matrixSource = args[2];
//			String outputPath = args[3];
//			int k = Integer.parseInt(args[4]);
//			double lambda = Double.parseDouble(args[5]);
//			int numIterations = Integer.parseInt(args[6]);
//			String[] splits = args[7].split("/");
//			String qSource = (splits[splits.length - 1].equals("-") ? "-"
//					: args[7]);// "-" means random Q generation
//			String whichSolver = args[8];

			boolean enableLogging = true; //TODO: originally it was false!
			String logPath = outputPath; //TODO: originally it was given as input parameter
			LogCreator logger = null;
			//String logPath = "";
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
								outputPath, k, lambda, numIterations, qSource,
								whichSolver);
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
								numIterations, k, lambda, qSource, whichSolver);
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
//	}

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
