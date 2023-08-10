package chapter_2

import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.util.Collector


case class Word(var word: String , var count: Int) {
  def this() = this("",0)
}


object Task1 extends App {


  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def countOfArticle() = {

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

    val countArticlesStream: DataStream[Word] =  onlyArticles
      .map(new MapFunction[String, Word] {
        override def map(value: String): Word = new Word(value,1)
      })
      .keyBy(new KeySelector[Word, String] {
        override def getKey(value: Word): String = value.word
      })
      .sum("count")

    countArticlesStream.print()
  }

  countOfArticle()
  env.execute()
}
