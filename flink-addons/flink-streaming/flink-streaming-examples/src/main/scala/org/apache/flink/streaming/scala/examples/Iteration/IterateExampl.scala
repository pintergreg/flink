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
package org.apache.flink.streaming.scala.examples.Iteration

import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * Example illustrating iterations in Flink streaming.
 *
 * <p>
 * The program sums up random numbers and counts additions it performs to reach
 * a specific threshold in an iterative streaming fashion.
 * </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>streaming iterations,
 * <li>buffer timeout to enhance latency,
 * <li>directed outputs.
 * </ul>
 */

object IterateExample {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }


    // setup input for the stream of (0,0) pairs
    //TODO


    // StreamExecution
    val  env = StreamExecutionEnvironment.getExecutionEnvironment
                .setBufferTimeout(1)

    val it = env.fromCollection(generateStream).shuffle.iterate(5000)

    val step = it.map(t => {val (x,y)= t ;(x.toString().toDouble() + Random.nextDouble(), y +1)})
                  .shuffle
                  .split(//)







    // emit result


    if (fileOutput) {
      it.writeAsText(outputPath, 1)
    }
    else {
      it.print
    }



    // execute the program


    env.execute("Streaming Iteration Example")

  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************


  def generateStream: Stream[(Double, Int)] ={
    Stream.from(0,1).map(x => (0.0,0))
  }


  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
      }
      else {
        System.err.println("Usage: IterateExample <result path>")
        false
      }
    }
    else {
      System.out.println("Executing IterateExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: IterateExample <result path>")
    }
    true
  }

  private var fileOutput : Boolean = false
  private var outputPath: String = null







}
