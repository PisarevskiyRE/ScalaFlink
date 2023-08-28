package chapter_5

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import scala.jdk.CollectionConverters._
import java.time.Instant

object Example2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    Event(startTime.plusMillis(1500L), "kA", "SA1.5"),
    Event(startTime.plusMillis(1700L), "kA", "SA1.7"),
    Event(startTime.plusMillis(2000L), "kB", "SB2"),
    Event(startTime.plusMillis(2100L), "kB", "SB2.1"),
    Event(startTime.plusMillis(3000L), "kC", "SC3"),
    Event(startTime.plusMillis(4000L), "kC", "SC4"),
    Event(startTime.plusMillis(7000L), "kC", "SC7"),
    Event(startTime.plusMillis(8000L), "kC", "SC8"),
    Event(startTime.plusMillis(8700L), "kA", "SA8.7"),
  ).asJava


  val outputTag = new OutputTag[String]("side-output") {}


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


  val mainStream = stream
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .process(new ProcessAllWindowFunction[Event, String, TimeWindow] {
      override def process(
                            context: ProcessAllWindowFunction[Event, String, TimeWindow]#Context,
                            elements: lang.Iterable[Event],
                            out: Collector[String]): Unit = {

        out.collect(s"window [${context.window().getStart} - ${context.window().getEnd}]" +
          s"has ${elements.asScala.size} elements: $elements")

        if (elements.asScala.size > 3) {
          context.output(
            outputTag,
            s"side output: window[${context.window().getStart} - ${context.window().getEnd}] " +
              s"has ${elements.asScala.size} elements")
        }

      }
    })


  mainStream.print()

  val sideOutputStream = mainStream.getSideOutput(outputTag)
  sideOutputStream.print()


  env.execute()
}
