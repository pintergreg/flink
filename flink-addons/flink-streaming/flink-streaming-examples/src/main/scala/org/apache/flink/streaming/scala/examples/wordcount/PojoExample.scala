package org.apache.flink.streaming.scala.examples.wordcount

import org.apache.flink.examples.java.wordcount.util.WordCountData
import org.apache.flink.streaming.api.scala._

/**
 * This example shows an implementation of WordCount without using the Tuple2
 * type, but a custom class.
 *
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use POJO data types,
 * <li>write a simple Flink program,
 * <li>write and use user-defined functions.
 * </ul>
 */

object PojoExample {
  case class Word (word: String, frequency: Int)

  def main(args: Array[String]) {

    if (!parseParameters(args)){
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = getTextDataStream(env)

    val counts = text.flatMap {_.toLowerCase.split("\\W+") filter {_.nonEmpty}}
                .map { Word(_, 1)}
                .groupBy("word")
                .sum("frequency")

    if (fileOutput) {
      counts.writeAsCsv(outputPath, 1)
    }else{
      counts.print()
    }
    env.execute("Scala WordCount example with Streaming")
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
      } else {
        System.err.println("Usage: PojoExample <text path> <result path>")
        false
      }
    } else {
      System.out.println("Executing PojoExample example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: PojoExample <text path> <result path>")
    }
    true
  }

  private def getTextDataStream(env: StreamExecutionEnvironment): DataStream[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    }else{
      env.fromCollection(WordCountData.WORDS)
    }
  }

  private var fileOutput : Boolean = false
  private var textPath: String = null
  private  var outputPath: String = null
}