package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data.{DslamMetadata, SdcAverage, SdcDataEnrichment, SdcEnrichedInstant}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 24/9/18.
  */
object IdentifyMissingDslam {
	class AggIncremental extends AggregateFunction[DslamMetadata, Long, Long] {
		override def createAccumulator() = 0L
		override def add(in: DslamMetadata, acc: Long): Long = scala.math.max(in.metricsTime, acc)
		override def merge(acc: Long, acc2: Long): Long = scala.math.max(acc, acc2)
		override def getResult(acc: Long): Long = acc
	}

	class AggFinal extends ProcessWindowFunction[Long, (Long, String), String, TimeWindow] {
		override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(Long, String)]): Unit = {
			out.collect(elements.iterator.next(), key)
		}
	}
}
