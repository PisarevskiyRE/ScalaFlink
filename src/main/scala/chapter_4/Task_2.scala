package chapter_4

import generator._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant

object Task_2 extends App{


  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val clicks: DataStreamSource[Click] = env.addSource(new ClickGenerator(1, startTime, 500))


  val clicksByUser = clicks.keyBy(new KeySelector[Click, String] {
    override def getKey(value: Click): String = value.userId
  })

  val filteredClicks  = clicksByUser
    .process(new KeyedProcessFunction[String, Click, Click] {
      private var lastClickCountState: ValueState[Int] = _

      override def open(parameters: Configuration): Unit = {
        val lastClickCountDescriptor = new ValueStateDescriptor("lastClickCount", classOf[Int])
        lastClickCountState = getRuntimeContext.getState(lastClickCountDescriptor)
      }

      override def processElement( value: Click,
                                   ctx: KeyedProcessFunction[String, Click, Click]#Context,
                                   out: Collector[Click]): Unit = {


        val lastClickCount = lastClickCountState.value()
        if (value.clickCount == 0 || Math.abs(value.clickCount - lastClickCount) > 5) {
          lastClickCountState.update(value.clickCount)
          out.collect(value)
        }
      }
    })

  filteredClicks.print()

  env.execute()
}


