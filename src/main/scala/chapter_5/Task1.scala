package chapter_5

import generator._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant


object Task1 extends App{
  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val startTime: Instant =
    Instant.parse("2023-07-30T00:00:00.000Z")

  val events: DataStreamSource[Event] = env.addSource(new EventGenerator(1, startTime, 10))

//  val eventByUser = events.keyBy((value: Event) => value.appId)
//
//  val clicksPerUser = clicksByUser.process(
//    new KeyedProcessFunction[String, Click, String] {
//      var numClicksPerUser: ValueState[Long] = _
//
//      override def open(parameters: Configuration): Unit = {
//        numClicksPerUser = getRuntimeContext.getState(
//          new ValueStateDescriptor[Long](
//            "clicksCounter",
//            classOf[Long]))
//      }
//
//      override def processElement(
//                                   value: Click,
//                                   ctx: KeyedProcessFunction[String, Click, String]#Context,
//                                   out: Collector[String])
//      : Unit = {
//
//        val currentClickCount = numClicksPerUser.value()
//        numClicksPerUser.update(currentClickCount + 1)
//
//        out.collect(s"user ${value.userId} made ${currentClickCount + 1} click(s)")
//
//      }
//    }
//  )

  events.print()
  env.execute()
}
