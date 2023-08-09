import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object SimpleApp extends App {

  def filterOddNumbers(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStreamSource[Int] = env.fromElements(1, 2, 3, 4, 5)

    val filteredNumbers = numbers.filter(num => num % 2 != 0)

    filteredNumbers.print()

    env.execute()
  }

  filterOddNumbers()
}
