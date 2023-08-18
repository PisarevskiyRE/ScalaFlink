package chapter_3
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}


object example_3 extends App{

  final case class Event(
                          eventTime: Instant,
                          eventName: String,
                          description: String) {

    def info(): String = s"[$eventName] $eventTime [$description]"
  }

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")


  val events = List(
    Event(startTime.plusMillis(1000L), "kA", "SA1"),
    Event(startTime.plusMillis(2000L), "kB", "SB2"),
    Event(startTime.plusMillis(4000L), "kC", "SC4"),
    Event(startTime.plusMillis(8000L), "kA", "SA8"),
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val stream = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        }))


  val sessionWindow = stream
    .windowAll(EventTimeSessionWindows.withGap(Time.seconds(2)))
    .apply(new AllWindowFunction[Event, String, TimeWindow] {
      override def apply(window: TimeWindow, values: lang.Iterable[Event], out: Collector[String]): Unit = {
        out.collect(s"window [${window.getStart} - ${window.getEnd}]" +
          s"has ${values.size} elements: $values")
      }
    })


  sessionWindow.print()

  env.execute()

}
