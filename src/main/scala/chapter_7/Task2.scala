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

case class Order private(id: String, orderTime: Instant)
object Order{
  def apply(id: String, orderTime: String): Order = new Order(id, Instant.parse(orderTime))
}


case class Delivery private(id: String, order_id: String, deliveryTime: Instant)
object Delivery{
  def apply(id: String, order_id: String, deliveryTime: String): Delivery = new Delivery(id, order_id, Instant.parse(deliveryTime))
}

object Task2 extends App{

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment

  val orders = List(
    Order("aaaaa", "2023-06-15T12:07:13.000Z"),
    Order("bbbbb", "2023-06-19T23:15:44.000Z"),
    Order("ccccc", "2023-06-03T14:35:05.000Z"),
    Order("ddddd", "2023-07-01T00:18:34.000Z"),
    Order("eeeee", "2023-06-30T13:55:21.000Z"),
  ).asJava

  val deliveries = List(
    Delivery("d923e0511", "aaaaa", "2023-06-16T14:11:54.000Z"),
    Delivery("d876e0521", "bbbbb", "2023-06-25T18:04:40.000Z"),
    Delivery("d653e1241", "ccccc", "2023-06-05T17:17:36.000Z"),
    Delivery("d923e0511", "ddddd", "2023-07-04T02:18:34.000Z"),
    Delivery("d745e1941", "eeeee", "2023-06-02T03:42:11.000Z"),
  ).asJava

  val ordersStream = env
    .fromCollection(orders)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
          override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
            element.orderTime.toEpochMilli
          }
        })
    )

  val deliveriesStream = env
    .fromCollection(deliveries)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Delivery] {
          override def extractTimestamp(element: Delivery, recordTimestamp: Long): Long = {
            element.deliveryTime.toEpochMilli
          }
        })
    )


  private var onDateCount: Int = 0
  private var outDateCount: Int = 0

  private val maxTimeDelivery: Int = 3 * 24 * 3600

  val processOrders = ordersStream
    .keyBy(new KeySelector[Order, String] {
        override def getKey(value: Order): String = value.id
      })
    .connect(
          deliveriesStream
            .keyBy(new KeySelector[Delivery, String] {
              override def getKey(value: Delivery): String = value.order_id
            })
        )
    .process(new KeyedCoProcessFunction[String, Order, Delivery, (String, String, Instant)] {
      private var deliveryTimeState: ValueState[Instant] = _

      override def open(parameters: Configuration): Unit = {

        val deliveryTimeStateDescriptor = new ValueStateDescriptor("deliveryTimeState", classOf[Instant])
        deliveryTimeState = getRuntimeContext.getState(deliveryTimeStateDescriptor)

      }

      override def processElement1(
                                    value: Order,
                                    ctx: KeyedCoProcessFunction[String, Order, Delivery, (String, String, Instant)]#Context,
                                    out: Collector[(String, String, Instant)]): Unit = {
        val deliveryTime = deliveryTimeState.value()
        val deliveryDeadline: Instant = value.orderTime.plusSeconds(maxTimeDelivery)
        if (deliveryTime.isBefore(deliveryDeadline)) {

          onDateCount += 1
          out.collect(
            (
              "[Заказ выполнен вовремя " + onDateCount.toString +"] " + ctx.getCurrentKey,
              "доставка с " + value.orderTime.toString + " по "+ deliveryDeadline.toString,
              deliveryTime
            )
          )

        }
        else{
          outDateCount += 1
          out.collect(
            (
              "[Заказ опоздал " +outDateCount.toString +"] " + ctx.getCurrentKey,
              "доставка с " + value.orderTime.toString + " по "+ deliveryDeadline.toString,
              deliveryTime
            )
          )
        }
      }

      override def processElement2(
                                    value: Delivery,
                                    ctx: KeyedCoProcessFunction[String, Order, Delivery, (String, String, Instant)]#Context,
                                    out: Collector[(String, String, Instant)]): Unit = {
        deliveryTimeState.update(value.deliveryTime)
      }
    })

  processOrders.print()
  env.execute()
}
