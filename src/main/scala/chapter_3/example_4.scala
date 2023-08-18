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
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}

object example_4 extends App {

  final case class Event(
                          eventTime: Instant,
                          eventName: String,
                          description: String,
                          value: Int) {

    def info(): String = s"[$eventName] $eventTime [$description] $value"
  }

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val events = List(
    Event(startTime.plusMillis(1000L), "kA", "SA1", -1),
    Event(startTime.plusMillis(1000L), "kB", "SB2", 12),
    Event(startTime.plusMillis(2000L), "kC", "SC6", 0),
    Event(startTime.plusMillis(6000L), "kA", "SA9", 100),
    Event(startTime.plusMillis(7000L), "kC", "SC9.6", -12),
    Event(startTime.plusMillis(8000L), "kC", "SC10", 150),
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val streamReduce = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        }))
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .reduce(new ReduceFunction[Event] {
      override def reduce(value1: Event, value2: Event): Event = {
        if (value1.value > value2.value) value2
        else value1
      }
    })


  class AverageValueFunction extends AggregateFunction[Event, (Int, Double), String] {
    override def createAccumulator(): (Int, Double) = (0, 0.0)

    override def add(value: Event, accumulator: (Int, Double)): (Int, Double) = (accumulator._1 + 1, accumulator._2 + value.value)

    override def getResult(accumulator: (Int, Double)): String = s"average value is ${accumulator._2 / accumulator._1}"

    override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = (a._1 + b._1, a._2 + b._2)
  }

  val streamAgg = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        }))
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .aggregate(new AverageValueFunction)


  final case class Result(minValue: Int, maxValue: Int)

  class MinMaxFunction extends ProcessAllWindowFunction[Event, Result, TimeWindow] {
    override def process(
                          context: ProcessAllWindowFunction[Event, Result, TimeWindow]#Context, elements: lang.Iterable[Event],
                          out: Collector[Result]): Unit = {

      val values: Iterable[Int] = elements.map(_.value)

      out.collect(
        Result(values.min, values.max)
      )
    }
  }

  val streamProces = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        }))
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .process(new MinMaxFunction)





  streamReduce.print()
  streamAgg.print()
  streamProces.print()






  env.execute()







}
