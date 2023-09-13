package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.math.Ordered.orderingToOrdered

object Task3 extends App {

  case class Record(
                     @JsonProperty("app_name") var app_name: String,
                     @JsonProperty("date") var date: String,
                     @JsonProperty("time") var time: String,
                     @JsonProperty("duration") var duration: String,
                   )


  case class AppUsage private(
                               app_name: String,
                               dateTime: Long,
                               duration: Duration
                             )
  object AppUsage{
    def apply(app_name: String, date: String, time: String, duration: String): AppUsage = {

      val dateTime = java.time.LocalDateTime.parse(date + "|" +time, DateTimeFormatter.ofPattern("dd/MM/yyyy'|'HH:mm:ss"))
      val parts = duration.split(":").map(_.toLong)

      new AppUsage(
        app_name,
        dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli,
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
      "csv-file"
    )


  val typedStream = stream
    .map(new MapFunction[Record, AppUsage] {
      override def map(value: Record): AppUsage = {
        AppUsage(value.app_name, value.date, value.time, value.duration)
      }
    })
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[AppUsage] {
            override def extractTimestamp(element: AppUsage, recordTimestamp: Long): Long = {
              element.dateTime
            }
          })
      )
    .keyBy(new KeySelector[AppUsage, String] {
      override def getKey(value: AppUsage): String = value.app_name
    })




  val totalUsage = typedStream
    .process(new KeyedProcessFunction[String, AppUsage, (String, Long)] {

      private var allTimeState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        val allTimeDescriptor: ValueStateDescriptor[Long] = new ValueStateDescriptor("allTimeState", classOf[Long])
        allTimeState = getRuntimeContext.getState(allTimeDescriptor)
      }

      override def processElement(
                                   value: AppUsage,
                                   ctx: KeyedProcessFunction[String, AppUsage, (String, Long)]#Context,
                                   out: Collector[(String, Long)]): Unit = {

        allTimeState.update(allTimeState.value() + value.duration.toSeconds)
        out.collect(value.app_name, allTimeState.value())
      }
    })





  val hourlyUsage = typedStream
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .process(new ProcessWindowFunction[AppUsage, (String, Long), String, TimeWindow] {
      override def process(
                            key: String,
                            context: ProcessWindowFunction[AppUsage, (String, Long), String, TimeWindow]#Context,
                            elements: lang.Iterable[AppUsage],
                            out: Collector[(String, Long)]): Unit = {
        out.collect("["+context.window().getStart + " | " +context.window().getEnd+"] ->"+key, elements.asScala.size)
      }
    })




  val continuousUsage = typedStream
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(1)) )
    .process(new ProcessWindowFunction[AppUsage, AppUsage, String, TimeWindow] {
      override def process(
                            key: String,
                            context: ProcessWindowFunction[AppUsage, AppUsage, String, TimeWindow]#Context,
                            elements: lang.Iterable[AppUsage],
                            out: Collector[AppUsage]): Unit = {
        val elemWithLongDuration =
          elements.asScala.toSeq.filter(
              x => x.duration.getSeconds >= 30 * 60
            )
        elemWithLongDuration.foreach(x => out.collect(x))
      }
    })

  val simpleContinuousUsage = typedStream.filter(new FilterFunction[AppUsage] {
    override def filter(value: AppUsage): Boolean = value.duration >= Duration.ofMinutes(30)
  })

  totalUsage.print()
/*
13> (Screen on (locked),1014)
8> (Phone,1572)
8> (Phone,1578)
8> (Phone,1593)
8> (Screen on (unlocked),1118)
13> (Screen on (locked),1018)
13> (Screen on (locked),1022)
 */
  hourlyUsage.print()
/*
16> ([1574787600000 | 1574791200000] ->Instagram,2)
14> ([1574798400000 | 1574802000000] ->Screen off (locked),1)
8> ([1574766000000 | 1574769600000] ->Screen on (unlocked),2)
16> ([1574791200000 | 1574794800000] ->Instagram,2)
16> ([1574794800000 | 1574798400000] ->WPS Office,1)
8> ([1574769600000 | 1574773200000] ->Screen on (unlocked),3)
16> ([1574798400000 | 1574802000000] ->Instagram,1)
8> ([1574769600000 | 1574773200000] ->Phone,1)
8> ([1574773200000 | 1574776800000] ->Screen on (unlocked),1)
8> ([1574776800000 | 1574780400000] ->Screen on (unlocked),2)
8> ([1574780400000 | 1574784000000] ->Screen on (unlocked),2)
*/


//  continuousUsage.print()
  simpleContinuousUsage.print()
/*
14> AppUsage(Screen off (locked),1574658974000,PT2H21M32S)
14> AppUsage(Screen off (locked),1574640161000,PT5H13M29S)
14> AppUsage(Screen off (locked),1574776415000,PT50M53S)
14> AppUsage(Screen off (locked),1574736807000,PT4H55M9S)
14> AppUsage(Screen off (locked),1574727571000,PT2H33M46S)
12> AppUsage(Screen off,1574584874000,PT35M36S)
16> AppUsage(Instagram,1574634735000,PT31M44S)
12> AppUsage(Screen off,1574784155000,PT1H30M)
12> AppUsage(Screen off,1574781785000,PT37M13S)
*/


  env.execute()
}
