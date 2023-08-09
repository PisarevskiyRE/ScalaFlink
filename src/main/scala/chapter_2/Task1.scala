package chapter_2

import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple2

object Task1 extends App {


  def countOfArticle() = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream: DataStreamSource[String] = env.fromElements(
      "The Apache Flink community is excited to announce the release of Flink ML!",
      "This release focuses on enriching Flink MLs feature engineering algorithms.",
      "The library now includes thirty three feature engineering algorithms,",
      "making it a more comprehensive library for feature engineering tasks."
    )


    val words: DataStream[String] = textStream.flatMap(
      new FlatMapFunction[String, String] {
        override def flatMap(value: String, out: Collector[String]): Unit = {
          value.toLowerCase().split("\\W+").foreach(out.collect)
        }
      }
    )

    val onlyArticles: DataStream[String] = words.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = (value == "the" || value == "a")
    })

    val countArticles =  onlyArticles
      .map(new MapFunction[String, (String, Int)] {
        override def map(value: String): (String, Int) = (value, 1)
      })
      .keyBy(new KeySelector[(String, Int), Boolean] {
        override def getKey(value: (String, Int)): Boolean =
          value._1 == "the"
      })
      .reduce((a, b) => (a._1, a._2 + b._2))



    countArticles.print()
    env.execute()
  }
  countOfArticle()

}
