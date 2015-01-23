package Test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
 * Created by kidio on 21/01/15.
 * -Xplugin:/Users/kidio/.m2/repository/org/scalamacros/paradise_2.10.4/2.1.0-M3/paradise_2.10.4-2.1.0-M3.jar -P:/Users/kidio/.m2/repository/org/scalamacros/paradise_2.10.4/2.1.0-M3/paradise_2.10.4-2.1.0-M3.jar
 */
object  Test {
  def main(args: Array[String]) {
    val WORDS = Array("flink", "Streaming")

    var env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(WORDS).print()

    env.execute()
  }
}