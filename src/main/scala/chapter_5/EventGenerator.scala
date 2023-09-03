package chapter_5

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.time.{LocalDateTime, ZoneOffset}

case class Event(
                  store: String,
                  appId: String,
                  eventType: String,
                  eventTime: Long
                )


class EventGenerator(
                      batchSize: Int,
                      millisBtwEvents: Int
                    ) extends RichSourceFunction[Event]{
  @volatile private var isRunning = true

  private def generateEvent(id: Long): Seq[Event] = {
    val events = (1 to batchSize)
      .map(_ =>
        Event(
          EventGenerator.getStore,
          EventGenerator.getAppId,
          EventGenerator.getEventType,
          LocalDateTime.now().atZone(ZoneOffset.UTC).toInstant().toEpochMilli
        )
      )
    events
  }

  private def run(
                   startId: Long,
                   ctx: SourceFunction.SourceContext[Event])
  : Unit = {

    while (isRunning) {
      generateEvent(startId).foreach(ctx.collect)
      Thread.sleep(batchSize * millisBtwEvents)
      run(startId + batchSize, ctx)
    }
  }

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = run(0, ctx)

  override def cancel(): Unit = isRunning = false

}
object EventGenerator {

  private val stores: Vector[String] =
    Vector(
      "HuaweiAppStore",
      "AppleAppStore",
      "GooglePlay",
      "RuStore"
    )

  private val appsId: Vector[String] =
    Vector(
      "2616001479570096825",
      "4091758100234781984",
      "9102759165103862720",
      "1045619379553910805"
    )

  private val eventTypes: Vector[String] =
    Vector(
      "page_view",
      "install",
      "uninstall",
      "error"
    )

  def getStore: String = stores(scala.util.Random.nextInt(stores.length))
  def getAppId: String = appsId(scala.util.Random.nextInt(appsId.length))
  def getEventType: String = eventTypes(scala.util.Random.nextInt(eventTypes.length))

}