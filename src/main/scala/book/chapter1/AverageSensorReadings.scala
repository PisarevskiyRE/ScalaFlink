package book.chapter1

import book.until.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object AverageSensorReadings {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000L)


    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
              element.timestamp
            }
          }))

    val avgTemp: DataStream[SensorReading] = sensorData
      .map( r =>
        SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      .keyBy(new KeySelector[SensorReading, String] {
        override def getKey(value: SensorReading): String = value.id
      })

      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new TemperatureAverager)


    avgTemp.print()

    env.execute("Compute average sensor temperature")
  }
}

class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: lang.Iterable[SensorReading], out: Collector[SensorReading]): Unit = {
    val (cnt, sum) = input.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
        val avgTemp = sum / cnt
        out.collect(SensorReading(key, window.getEnd, avgTemp))
  }
}
