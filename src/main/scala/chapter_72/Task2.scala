package chapter_72

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.nio.charset.StandardCharsets

case class Event(
                  user_id: Int,
                  item_id: Int,
                  category_id: Int,
                  behavior: String,
                  ts: String)

object Task2 extends App{



  val env: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment

  val kafkaSource: KafkaSource[Event] = KafkaSource.builder[Event]()
    .setBootstrapServers("localhost:9092")
    .setTopics("user_behavior")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new EventDeserializer())
    .build()


  val kafkaStream: DataStream[Event] = env.fromSource(
    kafkaSource,
    WatermarkStrategy.noWatermarks(),
    "Kafka-Source") //source name


  kafkaStream.print()

  env.execute()
}


class EventDeserializer extends DeserializationSchema[Event] {
  override def deserialize(message: Array[Byte]): Event = {

    val jsonString = new String(message, StandardCharsets.UTF_8)
    val sp = jsonString.split(",")

    Event(sp(1).toInt,sp(2).toInt,sp(3).toInt,sp(4),sp(5))
  }

  override def isEndOfStream(nextElement: Event): Boolean = false

  implicit val typeInfo = TypeInformation.of(classOf[Event])

  override def getProducedType: TypeInformation[Event] = implicitly[TypeInformation[Event]]
}