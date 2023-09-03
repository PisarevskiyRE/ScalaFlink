//package chapter_5
//
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.java.functions.KeySelector
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
//import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
//import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
//import org.apache.flink.util.Collector
//
//import java.lang
//import java.time.Instant
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//
//import scala.jdk.CollectionConverters.IterableHasAsScala
//
//object Task1_a extends App {
//
//  val env = StreamExecutionEnvironment
//    .getExecutionEnvironment
//
//  val startTime: Instant =
//    Instant.parse("2023-07-30T00:00:00.000Z")
//
//  val events = env.addSource(new EventGenerator(10, startTime, 5))
//    .assignTimestampsAndWatermarks(
//      WatermarkStrategy
//        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
//        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
//          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
//            element.eventTime.toEpochMilli
//          }
//        }))
//
//  val result = events
//    .keyBy(new KeySelector[Event, String] {
//      override def getKey(value: Event): String = value.store
//    })
//    .window(GlobalWindows.create())
//    .trigger(new CustomTrigger())
//    .evictor(CountEvictor.of(50, true))
//    .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {
//      override def process(
//                            key: String,
//                            context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
//                            elements: lang.Iterable[Event], out: Collector[String]): Unit = {
//        out.collect(s"Store: $key, Event Count: ${elements.asScala.size}")
//      }
//    })
//
//
//
//  result.print()
//  env.execute()
//}
//
//
//case class AppStats(installs: Int, uninstalls: Int, errors: Int)
//
//class CustomTrigger extends Trigger[Event, GlobalWindow] {
//  override def onElement(element: Event, timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE
//
//  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = ""
//}