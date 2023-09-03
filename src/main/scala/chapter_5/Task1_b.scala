//package chapter_5
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.functions.KeySelector
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows}
//import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
//import org.apache.flink.util.{Collector, OutputTag}
//
//import java.lang
//import java.time.Instant
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//
//import scala.jdk.CollectionConverters.IterableHasAsScala
//
//object Task1_b extends App{
//
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//  val errorOutputTag = new OutputTag[(String, Long)]("error-output"){}
//
//  val events = env.addSource(new EventGenerator(10, 5))
//    .assignTimestampsAndWatermarks(
//      WatermarkStrategy
//        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
//        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
//          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
//            element.eventTime
//          }
//        }))
//
//  val installUninstallCount = events
//    .keyBy(new KeySelector[Event, String] {
//      override def getKey(value: Event): String = value.store
//    })
//    .window(GlobalWindows.create())
//    .trigger(new CustomTrigger())
//    .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {
//
//            override def process( key: String,
//                                  context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
//                                  elements: lang.Iterable[Event],
//                                  out: Collector[String]): Unit = {
//
//              val topInstalls =
//                elements
//                  .asScala
//                  .filter(_.eventType == "install")
//                  .toSeq
//                  .sortBy( _.eventTime)
//                  .take(3)
//                  .map(_.appId)
//                  .mkString(", ")
//
//              val topUninstalls =
//                elements
//                  .asScala
//                  .filter(_.eventType == "uninstall")
//                  .toSeq
//                  .sortBy(_.eventTime)
//                  .take(3)
//                  .map(_.appId)
//                  .mkString(", ")
//
//              out.collect(s"Store: $key, Top Installs: [$topInstalls], Top Uninstalls: [$topUninstalls]")
//            }
//          })
//
//
//  val errorReport = events
//    .filter(_.eventType == "error")
//    .keyBy(new KeySelector[Event, String] {
//      override def getKey(value: Event): String = value.store
//    })
//    .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//    .process(
//      new ProcessWindowFunction[Event, (String, Long), String, TimeWindow] {
//        override def process(key: String, context: ProcessWindowFunction[Event, (String, Long), String, TimeWindow]#Context, elements: lang.Iterable[Event], out: Collector[(String, Long)]): Unit = {
//          val errorCount = elements.asScala.size
//
//          println("!!!"+(key, errorCount).toString())
//          out.collect((key, errorCount))
//        }
//    })
//
//  //installUninstallCount.print()
//
//  val errorStream: DataStream[(String, Long)] = errorReport.getSideOutput(errorOutputTag)
//  errorStream.print()
//
//  env.execute
//}
//
//case class AppStats(installs: Int, uninstalls: Int, errors: Int)
//
//class CustomTrigger extends Trigger[Event, GlobalWindow] {
//  override def onElement(
//                          element: Event,
//                          timestamp: Long,
//                          window: GlobalWindow,
//                          ctx: Trigger.TriggerContext)
//  : TriggerResult = {
//
//    val appStatsState: MapState[String, AppStats] = ctx.getPartitionedState(
//      new MapStateDescriptor[String, AppStats](
//        "appStats",
//        TypeInformation.of(classOf[String]),
//        TypeInformation.of(classOf[AppStats])
//      )
//    )
//
//    var appStats: AppStats = appStatsState.get(element.appId)
//
//    if (appStats == null) {
//
//      appStatsState.put(element.appId, AppStats(0, 0, 0))
//      appStats = appStatsState.get(element.appId)
//    }
//
//    val currentTimer = ctx.getCurrentProcessingTime + 1000
//    ctx.registerProcessingTimeTimer(currentTimer)
//
//    element.eventType match {
//      case "install" =>
//        appStatsState.put(element.appId, appStats.copy(installs = appStats.installs + 1))
//        if (appStats.installs >= 100) TriggerResult.FIRE else TriggerResult.CONTINUE
//      case "uninstall" =>
//        appStatsState.put(element.appId, appStats.copy(uninstalls = appStats.uninstalls + 1))
//        if (appStats.uninstalls >= 50) TriggerResult.FIRE else TriggerResult.CONTINUE
//      case "error" =>
//        appStatsState.put(element.appId, appStats.copy(errors = appStats.errors + 1))
//        TriggerResult.CONTINUE
//      case _ => TriggerResult.CONTINUE
//    }
//  }
//
//  override def onProcessingTime(
//                                 time: Long,
//                                 window: GlobalWindow,
//                                 ctx: Trigger.TriggerContext)
//  : TriggerResult = {
//
//    val appStatsState: MapState[String, AppStats] = ctx.getPartitionedState(
//      new MapStateDescriptor[String, AppStats](
//        "appStats",
//        TypeInformation.of(classOf[String]),
//        TypeInformation.of(classOf[AppStats])
//      )
//    )
//
//
//
//    TriggerResult.CONTINUE
//  }
//
//  override def onEventTime(
//                            time: Long,
//                            window: GlobalWindow,
//                            ctx: Trigger.TriggerContext)
//  : TriggerResult = TriggerResult.CONTINUE
//
//  override def clear(
//                      window: GlobalWindow,
//                      ctx: Trigger.TriggerContext): Unit = {
//    val state = ctx.getPartitionedState(
//      new MapStateDescriptor[String, AppStats](
//        "appStats",
//        TypeInformation.of(classOf[String]),
//        TypeInformation.of(classOf[AppStats])
//      )
//    )
//
//    state.clear()
//  }
//}
