package chapter_3

import scala.jdk.CollectionConverters._
import java.time.Instant

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.java.functions.KeySelector

object example_6 extends App {


  final case class Event(
                          eventTime: Instant,
                          eventName: String,
                          description: String) {

    def info(): String = s"[$eventName] $eventTime [$description]"
  }

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")


  val eventsA = List(
    Event(startTime.plusMillis(1000L), "kA", "SA1-1"),
    Event(startTime.plusMillis(1000L), "kB", "SA2-1"),
    Event(startTime.plusMillis(1000L), "kC", "SA3-1"),
    Event(startTime.plusMillis(3000L), "kA", "SA4-3"),
    Event(startTime.plusMillis(3000L), "kC", "SA5-3"),
    Event(startTime.plusMillis(2000L), "kC", "SA6-2"),
  ).asJava

  val eventsB = List(
    Event(startTime.plusMillis(0L), "kA", "SB1-0"),
    Event(startTime.plusMillis(0L), "kB", "SB2-0"),
    Event(startTime.plusMillis(0L), "kA", "SB6-0"),
    Event(startTime.plusMillis(1000L), "kC", "SB3-1"),
    Event(startTime.plusMillis(3000L), "kC", "SB4-3"),
    Event(startTime.plusMillis(4000L), "kA", "SB5-4"),
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val streamA = env
    .fromCollection(eventsA)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )

  val streamB = env
    .fromCollection(eventsB)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )

  val joinedStream = streamA
    .join(streamB)
    .where(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.eventName
    })
    .equalTo(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.eventName
    })
    .window(TumblingEventTimeWindows.of(Time.seconds(2L)))
    .apply(new JoinFunction[Event, Event, String]() {
      override def join(first: Event, second: Event): String = {
        s"join:\n" +
          s"${first.info()}\n" +
          s"${second.info()}\n"
      }
    })

  joinedStream.print()

  env.execute()

}
