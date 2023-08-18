package chapter_3

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.windowing.assigners. TumblingEventTimeWindows
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

object Task1 extends App {

  final case class Click(userId: String, clickTime: Instant)

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)

  val clicks = List(
    Click("550e8400", eventTime(1000L)),
    Click("550e6200", eventTime(1000L)),
    Click("550e1207", eventTime(1000L)),
    Click("550e8400", eventTime(2000L)),
    Click("550e6200", eventTime(2000L)),
    Click("550e1207", eventTime(3000L)),
    Click("550e8400", eventTime(4000L)),
    Click("550e1207", eventTime(4000L)),
    Click("550e6200", eventTime(4000L)),
    Click("550e1207", eventTime(8000L)),
    Click("550e6200", eventTime(8000L)),
    Click("550e1207", eventTime(9000L)),
    Click("550e1207", eventTime(10000L)),
    Click("550e1207", eventTime(11000L)),
    Click("550e6200", eventTime(11000L)),
    Click("550e1207", eventTime(12000L)),
    Click("550e8400", eventTime(12000L)),
    Click("550e1207", eventTime(18000L)),
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val resultStream: SingleOutputStreamOperator[String] = env
    .fromCollection(clicks)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
          override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
            element.clickTime.toEpochMilli
          }
        }))
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .process(new ClickCountProcessor)


   class ClickCountProcessor extends ProcessAllWindowFunction[Click, String, TimeWindow] {
    override def process(context: ProcessAllWindowFunction[Click, String, TimeWindow]#Context, elements: lang.Iterable[Click], out: Collector[String]): Unit = {
      val startTime = Instant.ofEpochMilli(context.window.getStart)
      val endTime = Instant.ofEpochMilli(context.window.getEnd)
      val totalCount = elements.size
      val output = s"Окно [$startTime - $endTime]: Кликов -> $totalCount"
      out.collect(output)
    }
  }

  resultStream.print()

  env.execute()
/*
11> Окно [2023-07-15T00:00:09Z - 2023-07-15T00:00:12Z]: Кликов -> 4
12> Окно [2023-07-15T00:00:12Z - 2023-07-15T00:00:15Z]: Кликов -> 2
10> Окно [2023-07-15T00:00:06Z - 2023-07-15T00:00:09Z]: Кликов -> 2
13> Окно [2023-07-15T00:00:18Z - 2023-07-15T00:00:21Z]: Кликов -> 1
9> Окно [2023-07-15T00:00:03Z - 2023-07-15T00:00:06Z]: Кликов -> 4
8> Окно [2023-07-15T00:00:00Z - 2023-07-15T00:00:03Z]: Кликов -> 5
*/

}
