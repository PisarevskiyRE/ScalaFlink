package chapter_3

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

object Task2 extends App {

  final case class Booking(geoRegion: String, eventTime: Instant, bookingCount: Int)

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)

  val clicks = List(
    Booking("Domestic", eventTime(1000L), 4),
    Booking("International", eventTime(1000L), 6),
    Booking("Domestic", eventTime(1000L), 10),
    Booking("International", eventTime(1200L), 7),
    Booking("International", eventTime(1800L), 12),
    Booking("International", eventTime(1500L), 4),
    Booking("International", eventTime(2000L), 1),
    Booking("Domestic", eventTime(2100L), 0),
    Booking("Domestic", eventTime(3000L), 2),
    Booking("International", eventTime(6000L), 8),
    Booking("International", eventTime(6000L), 1),
    Booking("Domestic", eventTime(6700L), 1),
    Booking("International", eventTime(7200L), 5),
    Booking("Domestic", eventTime(8000L), 3),
    Booking("International", eventTime(8100L), 6),
    Booking("Domestic", eventTime(8400L), 14),
    Booking("International", eventTime(9000L), 2),
    Booking("International", eventTime(9000L), 4),
  ).asJava

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val stream = env
    .fromCollection(clicks)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(100))
        .withTimestampAssigner(new SerializableTimestampAssigner[Booking] {
          override def extractTimestamp(element: Booking, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        }))

  class BookingProcessor extends ProcessWindowFunction[Booking, String, String, TimeWindow] {
    override def process(key: String,
                         context: ProcessWindowFunction[Booking, String, String, TimeWindow]#Context,
                         elements: lang.Iterable[Booking],
                         out: Collector[String]): Unit = {
      val geoRegion = key
            var maxBookingCount = 0
            var minBookingCount = 0

            elements.asScala.foreach { booking =>
              maxBookingCount = Math.max(maxBookingCount, booking.bookingCount)
              minBookingCount = Math.min(minBookingCount, booking.bookingCount)
            }

            val result = s"Тип-> $geoRegion Окно-> [${context.window.getStart} ${context.window.getEnd}] min-> $minBookingCount max-> $maxBookingCount"
            out.collect(result)
          }

  }


  val slidingWindow = stream
    .keyBy(new KeySelector[Booking, String] {
      override def getKey(value: Booking): String = value.geoRegion
    })
    .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
    .process(new BookingProcessor)




  slidingWindow.print()
  env.execute()


}
