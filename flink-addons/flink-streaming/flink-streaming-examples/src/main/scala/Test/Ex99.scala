package Test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.examples.twitter.util.TwitterStreamData

import scala.sys.process.BasicIO
import scala.util.parsing.json.JSON


/**
 * Created by kidio on 21/01/15.
 * -Xplugin:/Users/kidio/.m2/repository/org/scalamacros/paradise_2.10.4/2.1.0-M3/paradise_2.10.4-2.1.0-M3.jar -P:/Users/kidio/.m2/repository/org/scalamacros/paradise_2.10.4/2.1.0-M3/paradise_2.10.4-2.1.0-M3.jar
 */
object  Test {
  def main(args: Array[String]) {
    System.out.println(getString(TwitterStreamData.TEXTS(1), "lang"))
  }

  def getString(jsonText: String, field: String) :  String={
    val result = JSON.parseFull(jsonText)
    result match {
      case Some(map :Map[String, Any]) => map.getOrElse(field,"f").toString
      case None => "fail!"
    }
  }
  
  
}