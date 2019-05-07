package com.starfox.analysis.copper.sdc.flink.operator

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, ReduceFunction}
import org.apache.flink.streaming.api.functions.windowing.{AggregateApplyAllWindowFunction, ReduceApplyAllWindowFunction}
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 24/9/18.
  */
object CountDslam {
//	class AggIncremental extends AggregateFunction[(Long, String), (Long, Int), (Long, Int)] {
//		override def createAccumulator() = (0L, 0)
//		override def add(in: (Long, String), acc: (Long, Int)): (Long, Int) = (in._1, acc._2 + 1)
//		override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1, acc._2 + acc1._2)
//		override def getResult(acc: (Long, Int)): (Long, Int) = acc
//	}
	class AggIncremental extends AggregateFunction[(Long, String), Int, Int] {
	override def createAccumulator() = 0
	override def add(in: (Long, String), acc: Int): Int = acc + 1

	override def merge(acc: Int, acc1: Int): Int = acc + acc1

	override def getResult(acc: Int): Int = acc
	}

	class AggFinal extends ProcessAllWindowFunction[Int, (Long, Int), TimeWindow] {
		override def process(context: Context, elements: Iterable[Int], out: Collector[(Long, Int)]): Unit = {
			out.collect((context.window.getStart, elements.iterator.next()))
		}
	}
}
