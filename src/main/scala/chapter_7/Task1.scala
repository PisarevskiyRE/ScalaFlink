package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._

final case class Click(userId: String, clickTime: Instant)
final case class ClickCount(wStart: Long, wEnd: Long, clickCount: Int)

object Task1 extends App {

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val startTime: Instant =
    Instant.parse("1970-01-01T00:00:00Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)


  val clicks = List(
    Click("550e8400", eventTime(1000L)),
    Click("550e6200", eventTime(1000L)),
    Click("550e1207", eventTime(1000L)),
    Click("550e8400", eventTime(2000L)),
    Click("550e8400", eventTime(3000L)),
    Click("550e1207", eventTime(3000L)),
    Click("550e8400", eventTime(4000L)),
    Click("550e1207", eventTime(4000L)),
    Click("550e1208", eventTime(4000L)),
    Click("550e1209", eventTime(4000L)),
    Click("550e6200", eventTime(5000L)),
    Click("550e1207", eventTime(6000L)),
    Click("550e6200", eventTime(6000L)),
    Click("550e1207", eventTime(7000L)),
    Click("550e1207", eventTime(8000L)),
    Click("550e1207", eventTime(9000L)),
    Click("550e6200", eventTime(10000L)),
    Click("550e1207", eventTime(11000L)),
    Click("550e8400", eventTime(12000L)),
    Click("550e1207", eventTime(12000L)),
  ).asJava


  class TotalClicks extends ProcessAllWindowFunction[Click, ClickCount, TimeWindow] {
    override def process(
                          context: ProcessAllWindowFunction[Click, ClickCount, TimeWindow]#Context,
                          elements: lang.Iterable[Click],
                          out: Collector[ClickCount]): Unit = {

      out.collect(
        ClickCount(context.window().getStart, context.window().getEnd, elements.asScala.size)
      )
    }
  }

  val clicksStream = env.fromCollection(clicks)

  val slidingWindowStream = clicksStream
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
          override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
            element.clickTime.toEpochMilli
          }
        }))
    .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
    .process(new TotalClicks)
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
      .reduce(new ReduceFunction[ClickCount] {
        override def reduce(value1: ClickCount, value2: ClickCount): ClickCount = {
          if (value1.clickCount >= value2.clickCount) value1
          else value2
        }
      })


  slidingWindowStream.print()

  env.execute()
}
