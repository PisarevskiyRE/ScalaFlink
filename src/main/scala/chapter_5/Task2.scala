package chapter_5

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
import java.time.Instant


case class TimeOfDayData(date: Date, timeOfDay: String, count: Int)


object Task2 extends App{

  case class Record(
                     @JsonProperty("commentId") var commentId: String,
                     @JsonProperty("time") var time: Long,
                     @JsonProperty("user") var user: String,
                     @JsonProperty("topic") var topic: String,
                     @JsonProperty("acronym") var acronym: String
                   )

  def readCsv(): Unit = {
    val filePath = new Path("src/main/resources/acronyms.csv")

    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    val csvSchema = CsvSchema
      .builder()
      .addColumn("commentId")
      .addNumberColumn("time")
      .addColumn("user")
      .addColumn("topic")
      .addColumn("acronym")
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

    val hardwareComments = stream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(100))
          .withTimestampAssigner(new SerializableTimestampAssigner[Record] {
            override def extractTimestamp(element: Record, recordTimestamp: Long): Long = {
              element.time
            }
          }))
      .filter((x: Record) => x.topic == "Hardware")
      .map(new MapFunction[Record, TimeOfDayData] {
        override def map(value: Record): TimeOfDayData ={

          val hour = (value.time / 3600) % 24

          val timeOfDay = hour match {
            case h if h >= 4 && h < 12 => "Утро"
            case h if h >= 12 && h < 17 => "День"
            case h if h >= 17 && h < 24 => "Вечер"
            case _ => "Ночь"
          }

          val date = new Date(value.time * 1000)

          TimeOfDayData(date, timeOfDay, 1)
        }
      })



    val result = hardwareComments
      .keyBy(new KeySelector[TimeOfDayData, String] {
        override def getKey(value: TimeOfDayData): String = value.timeOfDay
      })
      .process(new KeyedProcessFunction[String, TimeOfDayData, String] {

        var valueState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          valueState = getRuntimeContext.getState(
            new ValueStateDescriptor[Long](
              "valueState",
              classOf[Long]))
        }


        override def processElement(
                                     value: TimeOfDayData,
                                     ctx: KeyedProcessFunction[String, TimeOfDayData, String]#Context,
                                     out: Collector[String])
        : Unit = {

          val currentClickCount = valueState.value()
          valueState.update(currentClickCount + 1)

          out.collect(s"[${value.date}] = ${value.timeOfDay} -> ${currentClickCount + 1}")
        }
      })

    result.print()
    env.execute()
  }
  readCsv()
}
