package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, FilterFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SideOutputDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.{Duration, Instant, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalUnit
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.math.Ordered.orderingToOrdered


object Task4 extends App {
  object Constant {
    val defaultFormat = "dd/MM/yyyy'|'HH:mm:ss"
    val sourceName = "csv-file"
  }

  case class Record(
                     @JsonProperty("app_name") var app_name: String,
                     @JsonProperty("date") var date: String,
                     @JsonProperty("time") var time: String,
                     @JsonProperty("duration") var duration: String,
                   )


  final case class AppUsage (
                               app_name: String,
                               startDate: Long,
                               endDate: Long,
                               duration: Duration
                             )
  object AppUsage{
    def apply(app_name: String, date: String, time: String, duration: String): AppUsage = {

      val dateTime = java.time.LocalDateTime.parse(
        date + "|" +time,
        DateTimeFormatter.ofPattern(
          Constant.defaultFormat
        )
      )

      val parts = duration.split(":").map(_.toLong)

      new AppUsage(
        app_name,
        dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli,
        dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli + Duration.ofHours(parts(0)).plusMinutes(parts(1)).plusSeconds(parts(2)).toMillis,
        Duration.ofHours(parts(0)).plusMinutes(parts(1)).plusSeconds(parts(2))
      )
    }
  }

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
      Constant.sourceName
    )

  val typedStream = stream
    .map(new MapFunction[Record, AppUsage] {
      override def map(value: Record): AppUsage = {
        AppUsage(value.app_name, value.date, value.time, value.duration)
      }
    })
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(100))
          .withTimestampAssigner(new SerializableTimestampAssigner[AppUsage] {
            override def extractTimestamp(element: AppUsage, recordTimestamp: Long): Long = {
              element.startDate
            }
          })
      )

  val isSleep = new OutputTag[AppUsage]("isSleep"){}

  val clearStream = typedStream
    .map(new MapFunction[AppUsage, AppUsage] {
      override def map(value: AppUsage): AppUsage =
        if (value.app_name.indexOf("Screen") >= 0)
          new AppUsage("Sleep", value.startDate,value.startDate+value.duration.toMillis, value.duration)
        else value
    })
    .keyBy(new KeySelector[AppUsage, Int] {
      override def getKey(value: AppUsage): Int = 1
    })
    .process(new KeyedProcessFunction[Int, AppUsage,AppUsage] {

      private var previousAppUsage: ValueState[AppUsage] = _

      override def open(parameters: Configuration): Unit = {

        val previousAppNameDescriptor: ValueStateDescriptor[AppUsage] = new ValueStateDescriptor("previousAppUsage", classOf[AppUsage])
        previousAppUsage = getRuntimeContext.getState(previousAppNameDescriptor)

      }

      override def processElement(
                                   value: AppUsage,
                                   ctx: KeyedProcessFunction[Int, AppUsage, AppUsage]#Context,
                                   out: Collector[AppUsage]): Unit = {

        val lastAppUsage = previousAppUsage.value() match {
          case null => new AppUsage("none", Long.MaxValue, Long.MinValue, Duration.ofMillis(0))
          case _ => previousAppUsage.value()
        }


        if (value.app_name == lastAppUsage.app_name){
          val mergedAppUsage = AppUsage(
            value.app_name,
            scala.math.min(
              lastAppUsage.startDate,
              value.startDate),
            scala.math.max(
              lastAppUsage.endDate,
              value.endDate),
            lastAppUsage.duration.plus(value.duration)
          )
          previousAppUsage.update(mergedAppUsage)
        } else {
          if (lastAppUsage.app_name != "none") out.collect(lastAppUsage)
          previousAppUsage.update(value)
        }
      }
    })


  val thresholdSleep = 1000* 60 * 60 * 4

  val filteredStream: SingleOutputStreamOperator[AppUsage] = clearStream
    .filter( x =>
      (
        x.app_name == "Sleep"
          && x.duration.toMillis >= thresholdSleep
        ))
    .keyBy(new KeySelector[AppUsage, Int] {
      override def getKey(value: AppUsage): Int = 1
    })
    .process(new KeyedProcessFunction[Int, AppUsage, AppUsage] {

      private var previousAppUsage: ValueState[AppUsage] = _

      override def open(parameters: Configuration): Unit = {

        val previousAppNameDescriptor: ValueStateDescriptor[AppUsage] = new ValueStateDescriptor("previousAppUsage", classOf[AppUsage])
        previousAppUsage = getRuntimeContext.getState(previousAppNameDescriptor)

      }

      override def processElement(
                                   value: AppUsage,
                                   ctx: KeyedProcessFunction[Int, AppUsage, AppUsage]#Context,
                                   out: Collector[AppUsage]): Unit = {

        val lastAppUsage = previousAppUsage.value()

        if (lastAppUsage != null) {
          if (value.startDate == lastAppUsage.endDate) {
            val mergedAppUsage = AppUsage(
              value.app_name,
              lastAppUsage.startDate,
              value.endDate,
              lastAppUsage.duration.plus(value.duration)
            )
            previousAppUsage.update(mergedAppUsage)
            out.collect(mergedAppUsage)
          } else {
            out.collect(value)

            previousAppUsage.update(lastAppUsage)
          }
        } else {
          previousAppUsage.update(value)
        }
      }
    })
//    .map(new MapFunction[AppUsage, String] {
//      override def map(value: AppUsage): String = {
//        "["+ Instant.ofEpochMilli(value.startDate) +"]-["+Instant.ofEpochMilli(value.endDate)+"] = "+value.duration.toHours
//
//      }
//    })


  val averageSleepDurationPerDay = filteredStream
    .keyBy(new KeySelector[AppUsage, Int] {
      override def getKey(value: AppUsage): Int = 1
    })
    .windowAll(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new AggregateFunction[AppUsage, (Duration, Long), String] {
      override def createAccumulator(): (Duration, Long) = (Duration.ofMillis(0), 0)

      override def add(value: AppUsage, accumulator: (Duration, Long)): (Duration, Long) = {
        val newDuration = accumulator._1.plus(value.duration)
        (newDuration, accumulator._2 + 1)
      }

      override def getResult(accumulator: (Duration, Long)): String = {
        val totalDuration = accumulator._1
        val totalRecords = accumulator._2

//        println(totalRecords)
//        println(totalDuration)


        val averageDurationHours = totalDuration.toMillis.toDouble / (totalRecords * 3600000) // Преобразование миллисекунд в часы
        s"Среднее время сна для всех записей: $averageDurationHours часов"
      }

      override def merge(a: (Duration, Long), b: (Duration, Long)): (Duration, Long) = {
        val mergedDuration = a._1.plus(b._1)
        (mergedDuration, a._2 + b._2)
      }
  })


  val sleepTimeWindow= filteredStream
    .keyBy(new KeySelector[AppUsage, Int] {
      override def getKey(value: AppUsage): Int = 1
    })
    .windowAll(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new AggregateFunction[AppUsage, (Duration, Long), String] {
      override def createAccumulator(): (Duration, Long) = (Duration.ZERO, 0L)

      override def add(value: AppUsage, accumulator: (Duration, Long)): (Duration, Long) = {


        val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(value.startDate), ZoneId.of("UTC"))

        val localTime: LocalTime = dateTime.toLocalTime

        val duration: Duration = Duration.ofHours(localTime.getHour).plusMinutes(localTime.getMinute).plusSeconds(localTime.getSecond)


        val newDuration = accumulator._1.plus(duration)
        (newDuration, accumulator._2 + 1)
      }

      override def getResult(accumulator: (Duration, Long)): String = {
        val totalDurationInMillis = accumulator._1
        val totalCount = accumulator._2

        val averageDurationInMillis = totalDurationInMillis.dividedBy(totalCount)
        averageDurationInMillis.toString
      }

      override def merge(a: (Duration, Long), b: (Duration, Long)): (Duration, Long) = {
        (a._1.plus(b._1), a._2 + b._2)
      }
    })


  val minMaxSleepDurationPerDay = filteredStream.
    map(new MapFunction[AppUsage, (String, AppUsage)] {
      override def map(value: AppUsage): (String, AppUsage) = {
        val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(value.startDate), ZoneId.of("UTC"))
        (dateTime.getDayOfWeek.toString, value)
      }
    })
    .keyBy(new KeySelector[(String, AppUsage), String] {
      override def getKey(value: (String, AppUsage)): String = value._1
    })
    .window(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new AggregateFunction[(String, AppUsage), (String, Long, Long), (String, Double)] {
      override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

      override def add(value: (String, AppUsage), accumulator: (String, Long, Long)): (String, Long, Long) = {
        val durationInSeconds = value._2.duration.getSeconds
        (value._1, accumulator._2 + durationInSeconds, accumulator._3 + 1)
      }

      override def getResult(accumulator: (String, Long, Long)): (String, Double) = {
        val dayOfWeek = accumulator._1
        val totalDurationInSeconds = accumulator._2
        val totalRecords = accumulator._3
        val averageDurationHours = totalDurationInSeconds.toDouble / (totalRecords * 3600)
        (dayOfWeek, averageDurationHours)
      }

      override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
        (a._1, a._2 + b._2, a._3 + b._3)
      }
    })







  //clearStream.print()
  //filteredStream.print()

  //averageSleepDurationPerDay.print()
  //sleepTimeWindow.print()
  minMaxSleepDurationPerDay.print()


  env.execute()
}
