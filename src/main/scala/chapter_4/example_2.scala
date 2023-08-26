package chapter_4

import generator._
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala

object example_2 extends App {
  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val clicks: DataStreamSource[Click] = env.addSource(new ClickGenerator(2, startTime, 500))

  val clicksByUser = clicks.keyBy(new KeySelector[Click, String] {
    override def getKey(value: Click): String = value.userId
  })

  val processedStream = clicksByUser.process(
    new KeyedProcessFunction[String, Click, String] {
      var listState: ListState[String] = _

      override def open(parameters: Configuration): Unit = {
        listState = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("btnTypesListState", classOf[String])
        )
      }

      override def processElement(
                                   value: Click,
                                   ctx: KeyedProcessFunction[String, Click, String]#Context,
                                   out: Collector[String])
      : Unit = {

        listState.add(value.button)

        val currentListState: Iterable[String] = listState.get().asScala
        out.collect(s"user ${value.userId} clicked ${currentListState.mkString(", ")} button(s)")
      }
    }

  )

  processedStream.print()

  env.execute()
}
