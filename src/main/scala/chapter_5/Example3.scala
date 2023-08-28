package chapter_5

import generator._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._

object Example3 extends App {
  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")


  val clicks: DataStreamSource[Click] = env.addSource(new ClickGenerator(1, startTime, 500))

  val lateOutputTag = new OutputTag[Click]("late-events") {}

  val mainStream: SingleOutputStreamOperator[String] = clicks
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
          override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
            element.time.toEpochMilli
          }
        }))
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
    .allowedLateness(Time.milliseconds(0))
    .sideOutputLateData(lateOutputTag)
    .apply(new AllWindowFunction[Click, String, TimeWindow] {
      override def apply(
                          window: TimeWindow,
                          values: lang.Iterable[Click],
                          out: Collector[String]): Unit = {
        out.collect(s"window [${window.getStart} - ${window.getEnd}]" +
          s"has ${values.asScala.size} elements: $values")
      }
    })


  val lateEventsStream = mainStream.getSideOutput(lateOutputTag)


  lateEventsStream
    .process(new ProcessFunction[Click, String] {
      override def processElement(
                                   value: Click,
                                   ctx: ProcessFunction[Click, String]#Context,
                                   out: Collector[String]): Unit = {
        out.collect(s"Late Data: $value")
      }
    })
    .print()

  env.execute()
}
