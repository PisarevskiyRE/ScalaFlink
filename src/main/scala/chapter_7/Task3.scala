package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Task3 extends App {

  case class Record(
                     @JsonProperty("app_name") var app_name: String,
                     @JsonProperty("date") var date: String,
                     @JsonProperty("time") var time: String,
                     @JsonProperty("duration") var duration: String,
                   )


  val filePath = new Path("src/main/resources/phone_usage.csv")

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val csvSchema = CsvSchema
    .builder()
    .addColumn("app_name")
    .addColumn("date")
    .addColumn("time")
    .addColumn("duration")
    .build()


  val source: FileSource[Record] = FileSource
    .forRecordStreamFormat(
      CsvReaderFormat.forSchema(csvSchema, Types.GENERIC(classOf[Record])),
      filePath)
    .build()

  val stream: DataStreamSource[Record] = env.
    fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "csv-file"
    )


  val timestampedStream = stream
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Record] {
          override def extractTimestamp(element: Record, recordTimestamp: Long): Long = {
            val dateTime = java.time.LocalDateTime.parse(element.date + "T" + element.time, DateTimeFormatter.ofPattern("dd/MM/yyyy'T'HH:mm:ss"))
            dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli
          }
        })
    )


  // 1. Статистика за все время: сколько часов пользователь использует каждое приложение
//  val totalUsage = timestampedStream
//    .keyBy(_.app_name)
//    .timeWindowAll(Time.hours(1))
//    .sum("duration.toLong")


/*
  // 2. Почасовая статистика: приложение, которое пользователь открывает чаще всего
  val hourlyUsage = timestampedStream
    .keyBy(_.app_name)
    .timeWindow(Time.hours(1))
    .aggregate(new AggregateFunction[Record, (String, Long), (String, Long)] {
      override def createAccumulator(): (String, Long) = ("", 0L)

      override def add(value: Record, accumulator: (String, Long)): (String, Long) = {
        val (appName, count) = accumulator
        (value.app_name, count + 1)
      }

      override def getResult(accumulator: (String, Long)): (String, Long) = accumulator

      override def merge(a: (String, Long), b: (String, Long)): (String, Long) = {
        (a._1, a._2 + b._2)
      }
    }, new WindowFunction[(String, Long), (String, Long), String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
        val result = input.maxBy(_._2).getOrElse(("", 0L))
        out.collect(result)
      }
    })

  // 3. Приложения с непрерывной длительностью использования более 30 минут в течение часа
  val continuousUsage = timestampedStream
    .keyBy(_.app_name)
    .timeWindow(Time.hours(1))
    .process(new ProcessWindowFunction[Record, (String, Long), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[Record], out: Collector[(String, Long)]): Unit = {
        val usage = elements.map(_.duration.toLong).sum
        if (usage >= 30 * 60 * 1000) { // 30 минут в миллисекундах
          out.collect((key, usage))
        }
      }
    })

  // Печатаем результаты
  totalUsage.print("Total Usage:")
  hourlyUsage.print("Hourly Usage:")
  continuousUsage.print("Continuous Usage:")

*/


  timestampedStream.print()

  env.execute()





}
