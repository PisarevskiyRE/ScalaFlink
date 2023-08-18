package chapter_3

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Instant
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.windowing.time.Time


object example_5 extends App{

  final case class Event(
                          eventTime: Instant,
                          eventName: String,
                          description: String) {

    def info(): String = s"[$eventName]: $eventTime $description"
  }

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val eventsA = List(
    Event(startTime.plusMillis(1000L), "B", "SA-B1-1"),
    Event(startTime.plusMillis(1000L), "B", "SA-B1-2"),
    Event(startTime.plusMillis(2000L), "B", "SA-B2"),
    Event(startTime.plusMillis(5000L), "B", "SA-B5"),
    Event(startTime.plusMillis(10000L), "B", "SA-B10"),
  ).asJava

  val eventsB = List(
    Event(startTime.plusMillis(2000L), "B", "SB-B2"),
    Event(startTime.plusMillis(3000L), "B", "SB-B3"),
    Event(startTime.plusMillis(8000L), "B", "SB-B8"),
    Event(startTime.plusMillis(10000L), "B", "SB-B10")
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val streamA = env
    .fromCollection(eventsA)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
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
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )


  val streamBA = streamB
    .keyBy(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.eventName
    })
    .intervalJoin(
      streamA
        .keyBy(new KeySelector[Event, String] {
          override def getKey(value: Event): String = value.eventName
        }))
    .between(Time.milliseconds(-2000L), Time.milliseconds(1000L))
    .process(new ProcessJoinFunction[Event, Event, String] {
      override def processElement(
                                   left: Event,
                                   right: Event,
                                   ctx: ProcessJoinFunction[Event, Event, String]#Context,
                                   out: Collector[String]): Unit = {

        out.collect(s"join:\n${left.info()}\n${right.info()}\n" +
          s"timestamp for joined events: ${ctx.getTimestamp} \n")
      }
    })

  streamBA.print()

  env.execute()
}
