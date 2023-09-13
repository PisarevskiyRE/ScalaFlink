package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant
import scala.jdk.CollectionConverters._

trait Event2 {
  def timestamp(): Instant

  def key(): String
}

case class Order2 private(id: String, orderTime: Instant) extends Event2 {
  override def timestamp(): Instant = orderTime

  override def key(): String = id
}

object Order2 {
  def apply(id: String, orderTime: String): Order2 = new Order2(id, Instant.parse(orderTime))
}

case class Delivery2 private(id: String, order_id: String, deliveryTime: Instant) extends Event2 {
  override def timestamp(): Instant = deliveryTime

  override def key(): String = order_id
}

object Delivery2 {
  def apply(id: String, order_id: String, deliveryTime: String): Delivery2 = new Delivery2(id, order_id, Instant.parse(deliveryTime))
}


object Task2_right extends App {


  object Data {

    val orders = List(
      Order2("aaaaa", "2023-06-15T12:07:13.000Z"),
      Order2("bbbbb", "2023-06-19T23:15:44.000Z"),
      Order2("ccccc", "2023-06-03T14:35:05.000Z"),
      Order2("ddddd", "2023-07-01T00:18:34.000Z"),
      Order2("eeeee", "2023-06-30T13:55:21.000Z"),
    ).asJava

    val deliveries = List(
      Delivery2("d923e0511", "aaaaa", "2023-06-16T14:11:54.000Z"),
      Delivery2("d876e0521", "bbbbb", "2023-06-25T18:04:40.000Z"),
      Delivery2("d653e1241", "ccccc", "2023-06-05T17:17:36.000Z"),
      Delivery2("d923e0511", "ddddd", "2023-07-04T02:18:34.000Z"),
      Delivery2("d745e1941", "eeeee", "2023-06-02T03:42:11.000Z"),
    ).asJava
  }

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment



  private val maxTimeDelivery: Int = 3 * 24 * 3600
  private var onDateCount: Int = 0
  private var outDateCount: Int = 0

  def watermarkStrategy[E <: Event2](): WatermarkStrategy[E] = {
    WatermarkStrategy
      .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
      .withTimestampAssigner(new SerializableTimestampAssigner[E] {
        override def extractTimestamp(element: E, recordTimestamp: Long): Long = {
          element.timestamp().toEpochMilli
        }
      })
  }

  class EventKey[E <: Event2] extends KeySelector[E, String] {
    override def getKey(value: E): String = value.key()
  }

  class DeliveryStatsFunction extends KeyedCoProcessFunction[String, Order2, Delivery2, (String, String, Instant)] {
    private var deliveryTimeState: ValueState[Instant] = _
    private var orderTimeState: ValueState[Instant] = _

    override def open(parameters: Configuration): Unit = {

      val deliveryTimeStateDescriptor = new ValueStateDescriptor("deliveryTimeState", classOf[Instant])
      deliveryTimeState = getRuntimeContext.getState(deliveryTimeStateDescriptor)

      val orderTimeStateDescriptor = new ValueStateDescriptor("orderTimeState", classOf[Instant])
      orderTimeState = getRuntimeContext.getState(deliveryTimeStateDescriptor)

    }

    override def processElement1(
                                  value: Order2,
                                  ctx: KeyedCoProcessFunction[String, Order2, Delivery2, (String, String, Instant)]#Context,
                                  out: Collector[(String, String, Instant)])
    : Unit = {

      val deliveryTime = deliveryTimeState.value()

      if (deliveryTime != null) {
        deliveryTimeState.clear()
        detectDeliveryStatus(deliveryTime, value.orderTime, ctx.getCurrentKey, out)
      } else {
        orderTimeState.update(value.orderTime)
      }
    }

    override def processElement2(
                                  value: Delivery2,
                                  ctx: KeyedCoProcessFunction[String, Order2, Delivery2, (String, String, Instant)]#Context,
                                  out: Collector[(String, String, Instant)])

    : Unit = {
      val orderTime = orderTimeState.value()
      if (orderTime != null) {
        orderTimeState.clear()
        detectDeliveryStatus(value.deliveryTime, orderTime, ctx.getCurrentKey, out)
      }
      else {
        deliveryTimeState.update(value.deliveryTime)
      }
    }

    private def detectDeliveryStatus(
                                      deliveryTime: Instant,
                                      orderTime: Instant,
                                      key: String,
                                      out: Collector[(String, String, Instant)])
    : Unit = {

      val deliveryDeadline: Instant = orderTime.plusSeconds(maxTimeDelivery)
      if (deliveryTime.isBefore(deliveryDeadline)) {
        onDateCount += 1
        out.collect(
          (
            "[Заказ выполнен вовремя " + onDateCount.toString + "] " + key,
            "доставка с " + orderTime.toString + " по " + deliveryDeadline.toString,
            deliveryTime
          )
        )

      } else {
        outDateCount += 1
        out.collect(
          (
            "[Заказ опоздал " + outDateCount.toString + "] " + key,
            "доставка с " + orderTime.toString + " по " + deliveryDeadline.toString,
            deliveryTime
          )
        )
      }
    }

  }

  val ordersStream = env
    .fromCollection(Data.orders)
    .assignTimestampsAndWatermarks(watermarkStrategy[Order2])

  val deliveriesStream = env
    .fromCollection(Data.deliveries)
    .assignTimestampsAndWatermarks(watermarkStrategy[Delivery2])

  val processedOrdersStream = ordersStream
    .keyBy(new EventKey[Order2])
    .connect(
      deliveriesStream
        .keyBy(new EventKey[Delivery2]))
    .process(new DeliveryStatsFunction)

  processedOrdersStream.print()
  env.execute()


  env.execute()
}
