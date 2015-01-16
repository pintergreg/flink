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

import java.io.Serializable;

import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.LinearSolverFactory;
import org.ejml.interfaces.linsol.LinearSolver;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import Jama.Matrix;

public class LinalgSolver implements Serializable {

	private static String[] solvers = {"jblas","jama","ejml"};
	
	
	public double[] solve(String whichSolver, int k, double[][] matrix,
			double[][] vector) {
		double[] result_elements = new double[k];

		if(whichSolver.equals("jblas")) {
			// JBLAS symmetric solver
			DoubleMatrix a = new DoubleMatrix(matrix);
			DoubleMatrix b = new DoubleMatrix(vector);
			DoubleMatrix result = Solve.solveSymmetric(a, b);
			for (int i = 0; i < k; ++i) {
				result_elements[i] = result.get(i, 0);
			}
		} else if(whichSolver.equals("jama")) {
			// Jama default solver
			Matrix a1 = new Matrix(matrix);
			Matrix b1 = new Matrix(vector);
			Matrix result1 = a1.solve(b1);
			for (int i = 0; i < k; ++i) {
				result_elements[i] = result1.get(i, 0);
			}
		} else if(whichSolver.equals("ejml")) {
			DenseMatrix64F a2 = new DenseMatrix64F(matrix);
			DenseMatrix64F b2 = new DenseMatrix64F(vector);
			DenseMatrix64F result2 = new DenseMatrix64F(k, 1);
			// TODO: Do I know that it is positive definit?
			LinearSolver<DenseMatrix64F> solver = LinearSolverFactory
					.symmPosDef(k);

			if (!solver.setA(a2)) {
				throw new IllegalArgumentException("Singular matrix");
			}
			solver.solve(b2, result2);

			for (int i = 0; i < k; ++i) {
				result_elements[i] = result2.get(i, 0);
			}
		} else {
			throw new IllegalArgumentException("There is no available linalg solver named "+ whichSolver + "! Available options "
					+ "are 'jama', 'jblas', 'ejml'.");
		}
		return result_elements;
	}
	
	public static boolean isLegalSolver(String whichSolver) {
		boolean legalSolver = false;
		for(String i : solvers) {
			if(whichSolver.equals(i)){
				legalSolver = true;
				break;
			}
		}
		return legalSolver;
	}

	public static String printOptions(String whichSolver) {
		String errorMsg = whichSolver
				+ " solver is not introduced! Available solvers:";
		int index = 0;
		for(String i : solvers) {
			++index;
			errorMsg += " "+i;
			errorMsg +=	(index == solvers.length ? "." : ",");
		}
		return errorMsg;
	}
}