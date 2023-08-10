package chapter_2

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2


object Task_2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def process() = {

    val inputStream: DataStreamSource[Tuple2[String, String]] = env.fromElements(
      new Tuple2("en", "car, bus, "),
      new Tuple2("ru", "машина, "),
      new Tuple2("en", "course, "),
      new Tuple2("en", "house, "),
    )


    val keyedInputStream = inputStream.keyBy(
      new KeySelector[Tuple2[String, String], String]{
        override def getKey(value: Tuple2[String, String]): String = {
          value.f0
        }
      }
    ).reduce(new ReduceFunction[Tuple2[String, String]] {
      override def reduce(value1: Tuple2[String, String], value2: Tuple2[String, String]): Tuple2[String, String] = {
        new Tuple2(value1.f0, value1.f1 + value2.f1)
      }
    })

    keyedInputStream.print()
  }
  process()
  env.execute()
}
