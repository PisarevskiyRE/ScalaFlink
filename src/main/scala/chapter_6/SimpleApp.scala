package chapter_6

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromElements(1, -2, -3, 4, 5)

    stream
      .filter(new FilterFunction[Int] {
        override def filter(value: Int): Boolean = value > 0
      })
      .print()

    env.execute()

  }
}
