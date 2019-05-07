package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 24/9/18.
  */
object IdentifyMissingDslam {
	def apply(streamDslamMetadata: DataStream[DslamRaw[None.type]],
			  missingThreshold: Int,
			  sdcDataInterval: Long = 900000,
			  allowedLateness: Long = 1800000): DataStream[(Long, String)] = {

		streamDslamMetadata
				.keyBy(_.name)
				.window(EventTimeSessionWindows.withGap(Time.minutes(missingThreshold * sdcDataInterval)))
				.allowedLateness(Time.minutes(allowedLateness))
				.aggregate(new IdentifyMissingDslam.AggIncremental, new IdentifyMissingDslam.AggFinal)
				.uid(OperatorId.STATS_MISSING_DSLAM)
				.name("Identify missing Dslam")
	}


	class AggIncremental extends AggregateFunction[DslamRaw[None.type], Long, Long] {
		override def createAccumulator() = 0L
		override def add(in: DslamRaw[None.type], acc: Long): Long = scala.math.max(in.ts, acc)
		override def merge(acc: Long, acc2: Long): Long = scala.math.max(acc, acc2)
		override def getResult(acc: Long): Long = acc
	}

	class AggFinal extends ProcessWindowFunction[Long, (Long, String), String, TimeWindow] {
		override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(Long, String)]): Unit = {
			out.collect(elements.iterator.next(), key)
		}
	}
}
