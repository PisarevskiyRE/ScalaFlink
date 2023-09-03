package chapter_5


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.{ ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

object Task1_c extends App{

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val events = env.addSource(new EventGenerator(10,5))
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime
          }
        }))

  val errorReport = events
    .filter( new FilterFunction[Event] {
      override def filter(value: Event): Boolean = value.eventType == "error"
    })
    .keyBy(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.store
    })
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .trigger(EventTimeTrigger.create())
    .apply(new WindowFunction[Event, (String, Long) ,String, TimeWindow]{
      override def apply(key: String, window: TimeWindow, input: lang.Iterable[Event], out: Collector[(String, Long)]): Unit = {
        val count = input.asScala.size
//        println( LocalDateTime.now())
//        println(input.asScala.mkString(", "))
        out.collect((s"[${window.getStart} - ${window.getEnd} = ${window.getStart-window.getEnd}]: " + key, count.toLong))
      }
    })


  val result = events
    .keyBy(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.store
    })
    .window(GlobalWindows.create())
    .trigger(new CustomTrigger())
    .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {
      override def process( key: String,
                            context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
                            elements: lang.Iterable[Event],
                            out: Collector[String]): Unit = {

        val topInstalls =
          elements
            .asScala
            .filter(_.eventType == "install")
            .toSeq
            .sortBy( _.eventTime)
            .take(3)
            .map(_.appId)
            .mkString(", ")

        val topUninstalls =
          elements
            .asScala
            .filter(_.eventType == "uninstall")
            .toSeq
            .sortBy(_.eventTime)
            .take(3)
            .map(_.appId)
            .mkString(", ")

        out.collect(s"Магазин: $key, Топ установок: [$topInstalls], Топ удалений: [$topUninstalls]")
      }
    })

  result.print()
  errorReport.print()

  env.execute()
}

case class AppStats(installs: Int, uninstalls: Int)

class CustomTrigger extends Trigger[Event, GlobalWindow] {
  override def onElement(
                          element: Event,
                          timestamp: Long,
                          window: GlobalWindow,
                          ctx: Trigger.TriggerContext
                        ): TriggerResult = {

    val appStatsState: MapState[String, AppStats] = ctx.getPartitionedState(
      new MapStateDescriptor[String, AppStats](
        "appStats",
        TypeInformation.of(classOf[String]),
        TypeInformation.of(classOf[AppStats])
      )
    )

    var appStats: AppStats = appStatsState.get(element.appId)

    if (appStats == null) {
      appStatsState.put(element.appId, AppStats(0, 0))
      appStats = appStatsState.get(element.appId)
    }

    element.eventType match {
      case "install" =>
        appStatsState.put(element.appId, appStats.copy(installs = appStats.installs + 1))
      case "uninstall" =>
        appStatsState.put(element.appId, appStats.copy(uninstalls = appStats.uninstalls + 1))
      case _ =>
    }

    if (appStats.installs >= 100 || appStats.uninstalls >= 50) TriggerResult.FIRE else TriggerResult.CONTINUE
  }

  override def onProcessingTime(
                                 time: Long,
                                 window: GlobalWindow,
                                 ctx: Trigger.TriggerContext
                               ): TriggerResult = {

      TriggerResult.CONTINUE
  }
  override def onEventTime(
                            time: Long,
                            window: GlobalWindow,
                            ctx: Trigger.TriggerContext
                          ): TriggerResult = TriggerResult.CONTINUE

  override def clear(
                      window: GlobalWindow,
                      ctx: Trigger.TriggerContext
                    ): Unit = {
    val state = ctx.getPartitionedState(
      new MapStateDescriptor[String, AppStats](
        "appStats",
        TypeInformation.of(classOf[String]),
        TypeInformation.of(classOf[AppStats])
      )
    )
    state.clear()
  }
}
