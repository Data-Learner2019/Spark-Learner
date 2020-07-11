import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem

object BoundStreamDemo {
  def main(args: Array[String]): Unit = {
    //    1. 初始化环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    2. 加载数据
    val lines:DataSet[String] = env.readTextFile("D:\\code\\java\\FlinkTutorial\\src\\main\\resources\\word.txt")
    lines.print()
    //    3. 指定操作数据的对应算子
    import org.apache.flink.api.scala._
    val dataSet: DataSet[(String, Int)] = lines.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1) //设置一个并行度，可以达到全局排序的效果
    //    4. 数据的持久化
    dataSet.print() // 在有界流中，print会触发action
    dataSet.writeAsText("D:\\code\\java\\FlinkTutorial\\src\\main\\resources\\BoundStreamDemo\\final_result.txt", FileSystem.WriteMode.OVERWRITE)
    //    5. 调用Execute触发执行程序
    env.execute("BoundStreamDemo")
  }
}
