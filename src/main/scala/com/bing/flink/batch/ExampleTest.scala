package com.bing.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment

object ExampleTest {

  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    wordCount1(env)
  }

  def wordCount1(env:ExecutionEnvironment):Unit={
    import org.apache.flink.api.scala._
    val data=env.readTextFile("E:\\data\\flink\\input\\file.txt")
    val wordCount=data.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCount.writeAsCsv("E:\\data\\flink\\output\\test.csv", "\n", " ")
             .setParallelism(1)
    env.execute("Batch word count")
  }
}
