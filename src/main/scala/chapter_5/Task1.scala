package chapter_5

import generator._
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._


object Task1 extends App{
  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  val startTime: Instant =
    Instant.parse("2023-07-30T00:00:00.000Z")

  val events: DataStreamSource[Event] = env.addSource(new EventGenerator(10, startTime, 5))


  val result: DataStream[String] = events
    .keyBy(new KeySelector[Event, String] {
      override def getKey(value: Event): String = value.store
    })
    .window(GlobalWindows.create())

    .trigger(new CustomTrigger())
    .evictor(CountEvictor.of(50, true))
    .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {

      override def process( key: String,
                            context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
                            elements: lang.Iterable[Event],
                            out: Collector[String]): Unit = {
        val topInstalls = elements.asScala
                  .filter(_.eventType == "install")
                  .toSeq
                  .sortBy( _.eventTime)
                  .take(3)
                  .map(_.appId)
                  .mkString(", ")

                val topUninstalls = elements.asScala
                  .filter(_.eventType == "uninstall")
                  .toSeq
                  .sortBy(_.eventTime)
                  .take(3)
                  .map(_.appId)
                  .mkString(", ")

                out.collect(s"Store: $key, Top Installs: [$topInstalls], Top Uninstalls: [$topUninstalls]")

      }
    })


  result.print()
  env.execute()
}

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueStateDescriptor}



case class AppStats(installs: Int, uninstalls: Int, errors: Int)

class CustomTrigger extends Trigger[Event, GlobalWindow] {
  override def onElement(
                          element: Event,
                          timestamp: Long,
                          window: GlobalWindow,
                          ctx: Trigger.TriggerContext)
  : TriggerResult = {

    val appStatsState: MapState[String, AppStats] = ctx.getPartitionedState(
      new MapStateDescriptor[String, AppStats](
        "appStats",
        TypeInformation.of(classOf[String]),
        TypeInformation.of(classOf[AppStats])
      )
    )

    val appStats: AppStats = appStatsState.get(element.appId)

    if (appStats == null) {
      appStatsState.put(element.appId, AppStats(0, 0, 0))
    }

    element.eventType match {
      case "install" =>
        appStatsState.put(element.appId, appStats.copy(installs = appStats.installs + 1))
        if (appStats.installs >= 100) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
      case "uninstall" =>
        appStatsState.put(element.appId, appStats.copy(uninstalls = appStats.uninstalls + 1))
        if (appStats.uninstalls >= 50) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
      case "error" =>
        TriggerResult.FIRE // Trigger immediately on errors
      case _ => TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(
                                 time: Long,
                                 window: GlobalWindow,
                                 ctx: Trigger.TriggerContext)
  : TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(
                            time: Long,
                            window: GlobalWindow,
                            ctx: Trigger.TriggerContext)
  : TriggerResult = TriggerResult.CONTINUE

  override def clear(
                      window: GlobalWindow,
                      ctx: Trigger.TriggerContext): Unit = {
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

