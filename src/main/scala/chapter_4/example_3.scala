package chapter_4

import generator._
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala


object example_3 extends App{
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
      var mapState: MapState[String, Int] = _

      override def open(parameters: Configuration): Unit = {
        mapState = getRuntimeContext.getMapState(
          new MapStateDescriptor[String, Int]("mapStateCounter", classOf[String], classOf[Int])
        )

      }

      override def processElement(
                                   value: Click,
                                   ctx: KeyedProcessFunction[String, Click, String]#Context,
                                   out: Collector[String])
      : Unit = {

        val btnType = value.button

        if (mapState.contains(btnType)) {
          val currentCount = mapState.get(btnType) + 1
          mapState.put(btnType, currentCount)
        } else mapState.put(btnType, 1)


        // ctx.getCurrentKey дает то же значение, что и value.userId
        out.collect(s"user ${ctx.getCurrentKey} clicked " +
          s"${mapState.entries().asScala.mkString(", ")} button(s)")
      }
    }

  )

  processedStream.print()

  env.execute()
}
