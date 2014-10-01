/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package org.apache.flink.examples.scala.wordcount

import org.apache.flink.api.common.expressions.{UnresolvedFieldReference, Literal}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.expressions._
import org.apache.flink.examples.java.wordcount.util.WordCountData

import scala.language.implicitConversions


/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files. 
 *
 * The input is a plain text file with lines separated by newline characters.
 *
 * Usage:
 * {{{
 *   WordCount <text path> <result path>>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.wordcount.util.WordCountData]]
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 *
 */

//case class WC(word: String, count: Int)

object ExpressionTest {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(1)
    val text = getTextDataSet(env)

    val numbers = env.fromElements((1, 1, "a"), (1, 2, "b"), (1, 4, "c"), (2, 4, "d")).as('key, 'value, 'n)
//    val numbers = env.fromElements((1, 1, "a"), (1, 2, "b"), (1, 4, "c"), (2, 4, "d")).select('_1 as 'key, '_2 as 'value, '_3 as 'n)

//    val result = numbers.group('key).select('key, 'n)
    val result = numbers.select('key, ('key as 'foo) + " Hello", ('value.avg + "Average").substring(0, 'value.avg + 5), 'value.sum + 1L, 'n.count, 'value.min, 'value.max, 5.toByte)
//    val result = numbers.filter(!('key))
//    val result = numbers.select('key, 'value.count, 'value.count, 'value.avg, 'value.avg, 'value)
//    val result = numbers.join(numbers).where('left$key === 'right$key).select('left$key, 'left$value.avg)

//    val numbers = env.fromElements(("a", Seq(1,2,3)))

//    val result = numbers.select('_1 + " Hello", '_2)

    result.print()



//    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
//      .map { (_, 1) }.select('_1 as 'word, '_2 as 'count)
//      .group('_1)
//      .sum(1)
//      .as[WC]
//
//    val counts2 = counts

//    val res = counts.join(counts2)
//      .where('left$word === 'right$word && ('left$count > 3 || 'left$count > -2))
//      .select('left$count as 'count, 'right$word as 'word)
//      .filter('word === "that" || 'count > 3)
//      .as[WC] map { wc => WC(wc.word, wc.count + 100) }


//    val input = env.fromElements(("a", 1), ("a", 2), ("b", 2), ("b", 3))
//    val res = input.group('_1).select('_1 + " Hello", '_2.sum, '_2.min, '_2.max)
//
//    res.print()

//    res2.print()

//    if (fileOutput) {
//      counts.writeAsCsv(outputPath, "\n", " ")
//    } else {
//      counts.print()
//    }

    env.execute("Expression Test")
//    println(env.getExecutionPlan())
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
      } else {
        System.err.println("Usage: WordCount <text path> <result path>")
        false
      }
    } else {
      System.out.println("Executing WordCount example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: WordCount <text path> <result path>")
    }
    true
  }

  private def getTextDataSet(env: ExecutionEnvironment): DataSet[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    }
    else {
      env.fromCollection(WordCountData.WORDS)
    }
  }

  private var fileOutput: Boolean = false
  private var textPath: String = null
  private var outputPath: String = null
}


