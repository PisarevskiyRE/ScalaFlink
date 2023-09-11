package chapter_7

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.collection.mutable
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
    Order("a", "2023-06-15T12:07:13.000Z"),
    Order("b", "2023-06-19T23:15:44.000Z"),
    Order("c", "2023-06-03T14:35:05.000Z"),
    Order("d", "2023-07-01T00:18:34.000Z"),
    Order("e", "2023-06-30T13:55:21.000Z"),
  ).asJava

  val deliveries = List(
    Delivery("d923e0511", "a", "2023-06-16T14:11:54.000Z"),
    Delivery("d876e0521", "b", "2023-06-25T18:04:40.000Z"),
    Delivery("d653e1241", "c", "2023-06-05T17:17:36.000Z"),
    Delivery("d923e0511", "d", "2023-07-04T02:18:34.000Z"),
    Delivery("d745e1941", "e", "2023-06-02T03:42:11.000Z"),
  ).asJava




  val ordersStream = env
    .fromCollection(orders)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
          override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
            element.orderTime.toEpochMilli
          }
        }))

  val deliveriesStream = env
    .fromCollection(deliveries)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Delivery] {
          override def extractTimestamp(element: Delivery, recordTimestamp: Long): Long = {
            element.deliveryTime.toEpochMilli
          }
        }))


//  ordersStream.print()
//  deliveriesStream.print()


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
    .process(new CoProcessFunction[Order, Delivery, (String, Instant, Instant)] {



      override def processElement1(
                                    value: Order,
                                    ctx: CoProcessFunction[Order, Delivery, (String, Instant, Instant)]#Context,
                                    out: Collector[(String, Instant, Instant)]): Unit = {
        val deliveryTime = Instant.ofEpochMilli(ctx.timerService().currentWatermark())
        val deliveryDeadline: Instant = value.orderTime.plusSeconds(3 * 24 * 3600)
        if (deliveryTime.isBefore(deliveryDeadline)) {
          println("|=" + value.id +"=|=" + value.orderTime.toString + "=|=" +deliveryDeadline.toString + "=|=" + deliveryTime.toString)






          out.collect((value.id, deliveryDeadline, deliveryTime))
        }
      }
      override def processElement2(
                                    value: Delivery,
                                    ctx: CoProcessFunction[Order, Delivery, (String, Instant, Instant)]#Context,
                                    out: Collector[(String, Instant, Instant)]): Unit = {

        ctx.getKeyedStateStore.getState

      }
    })

  processOrders.print()
  env.execute()

}
