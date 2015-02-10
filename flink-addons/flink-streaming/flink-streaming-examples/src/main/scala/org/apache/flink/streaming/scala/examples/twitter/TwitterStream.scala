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

package org.apache.flink.streaming.scala.examples.twitter

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.examples.twitter.util.TwitterStreamData
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.parsing.json.JSON

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON files in a streaming fashion.
 *
 * <p>
 * The input is a JSON text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>TwitterStream &lt;text path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link TwitterStreamData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 *
 */
object TwitterStream {


  def main(args: Array[String]) {
    
    if (!parseParameters(args)) {
      return
    }
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setBufferTimeout(1000)
    
    val streamSource = getTextDataStream(env)

    val tweets = streamSource.flatMap(enTweetTokenize _)
                  .map {(_,1)}
                  .groupBy(0)
                  .sum(1)

        // emit result
        if (fileOutput) {
          tweets.writeAsText(outputPath, 1)
        }
        else {
          tweets.print
        }
    
        // execute program
        env.execute("Twitter Streaming Example")

  }
  
  def enTweetTokenize (tweet: String, out:Collector[String]) = {
    val tweetObj = JSON.parseFull(tweet)
    if (getJsonString(tweetObj, "lang").equals("en")){
      val content = getJsonString(tweetObj,"text")
      content.toLowerCase().split("\\W+") filter (_.nonEmpty) foreach(out.collect(_))
    }

  }
  def getJsonString(jsonObj: Option[Any], field: String) :  String={
    jsonObj match {
      case Some(map : Map[String,Any])=> map.getOrElse(field, "f").toString
      case None => "Not found"
    }
  }
  
  private def getTextDataStream (env : StreamExecutionEnvironment) : DataStream[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    }else{
      env.fromCollection(TwitterStreamData.TEXTS)
    }
  }

  private def parseParameters(args: Array[String]) = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
      }
      else {
        System.err.println("USAGE: TwitterStream <pathToPropertiesFile> <result path>")
        false
      }
    }
    else {
      System.out.println("Executing TwitterStream example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  USAGE: TwitterStream <pathToPropertiesFile>")
    }
     true
  }

  private var fileOutput : Boolean = false
  private var  textPath : String = null
  private var outputPath : String = null
  
}
