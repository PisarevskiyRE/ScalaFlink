package chapter_5

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty

object Reader {

  case class Record(
                     @JsonProperty("value") var value: String,
                     @JsonProperty("id") var id: Int)

  def readCsv(): Unit = {
    val filePath = new Path("src/main/resources/example.csv")

    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    val csvSchema = CsvSchema
      .builder()
      .addColumn("value")
      .addNumberColumn("id")
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


    stream.print()

    env.execute()

  }

  def main(args: Array[String]): Unit = {
    readCsv()
  }

}