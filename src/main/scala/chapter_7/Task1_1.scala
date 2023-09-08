package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._



object Task1_1 extends App {

  final case class Click(userId: String, clickTime: Instant)

  final case class ClickCount(wStart: Long, wEnd: Long, clickCount: Int)

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val startTime: Instant =
    Instant.parse("1970-01-01T00:00:00Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)


  val clicks = List(
    Click("550e8400", eventTime(1000L)),
    Click("550e6200", eventTime(1000L)),
    Click("550e1207", eventTime(1000L)),
    Click("550e8400", eventTime(2000L)),
    Click("550e8400", eventTime(3000L)),
    Click("550e1207", eventTime(3000L)),
    Click("550e8400", eventTime(4000L)),
    Click("550e1207", eventTime(4000L)),
    Click("550e1208", eventTime(4000L)),
    Click("550e1209", eventTime(4000L)),
    Click("550e6200", eventTime(5000L)),
    Click("550e1207", eventTime(6000L)),
    Click("550e6200", eventTime(6000L)),
    Click("550e1207", eventTime(7000L)),
    Click("550e1207", eventTime(8000L)),
    Click("550e1207", eventTime(9000L)),
    Click("550e6200", eventTime(10000L)),
    Click("550e1207", eventTime(11000L)),
    Click("550e8400", eventTime(12000L)),
    Click("550e1207", eventTime(13000L)),
  ).asJava


  class TotalClicks extends ProcessAllWindowFunction[Click, ClickCount, TimeWindow] {
    override def process(
                          context: ProcessAllWindowFunction[Click, ClickCount, TimeWindow]#Context,
                          elements: lang.Iterable[Click],
                          out: Collector[ClickCount]): Unit = {

      out.collect(
        ClickCount(context.window().getStart, context.window().getEnd, elements.asScala.size)
      )
    }
  }

  val clicksStream = env.fromCollection(clicks)

  val slidingWindowStream = clicksStream
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
          override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
            element.clickTime.toEpochMilli
          }
        }))
    .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
    .process(new TotalClicks)
    .windowAll(GlobalWindows.create())
    .trigger(new CustomTrigger())
    .process(new ProcessAllWindowFunction[ClickCount, ClickCount, GlobalWindow]{
      override def process(context: ProcessAllWindowFunction[ClickCount, ClickCount, GlobalWindow]#Context,
                           elements: lang.Iterable[ClickCount],
                           out: Collector[ClickCount]): Unit = {
        val maxClicks: ClickCount = elements.asScala.toSeq.sortBy(x=> - x.clickCount).head

        out.collect(maxClicks)
      }
    })


  class CustomTrigger extends Trigger[ClickCount, GlobalWindow] {
    var clickCounterState: ReducingState[Int] = _

    override def onElement(
                            element: ClickCount,
                            timestamp: Long,
                            window: GlobalWindow,
                            ctx: Trigger.TriggerContext): TriggerResult = {
      if (clickCounterState == null) {
        clickCounterState = ctx.getPartitionedState(
          new ReducingStateDescriptor[Int]("clickCounter", new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1 + value2
          }, classOf[Int])
        )
      }


      clickCounterState.add(1)
      val currentClickCount = clickCounterState.get()

      if (currentClickCount >= element.clickCount) {
        TriggerResult.FIRE
      } else {
        TriggerResult.CONTINUE
      }
    }

    override def onProcessingTime(
                                   time: Long,
                                   window: GlobalWindow,
                                   ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(
                              time: Long,
                              window: GlobalWindow,
                              ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(
                        window: GlobalWindow,
                        ctx: Trigger.TriggerContext): Unit = {
      clickCounterState.clear()
    }
  }

  slidingWindowStream.print()
  env.execute()
}
