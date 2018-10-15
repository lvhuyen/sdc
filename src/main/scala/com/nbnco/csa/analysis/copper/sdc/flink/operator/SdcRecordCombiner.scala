package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data.{SdcCombined, SdcEnrichedBase, SdcEnrichedHistorical, SdcEnrichedInstant}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector


/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This ProcessFunction is used to combine Historical with Instant SDC records
  */
class SdcRecordCombiner(val instantDataWait: Long, val historicalDataWait: Long, cacheTTL: Long) extends CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined] {
	lazy val cache: ValueState[SdcEnrichedBase] = getRuntimeContext.getState(new ValueStateDescriptor[SdcEnrichedBase]("Cache", classOf[SdcEnrichedBase]))
	lazy val triggered: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("Triggered", classOf[Boolean]))

	override def processElement1(in1: SdcEnrichedInstant, context: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#Context, collector: Collector[SdcCombined]): Unit = {
		cache.value() match {
			case h: SdcEnrichedHistorical =>
				collector.collect(SdcCombined(in1, h))
				cache.clear()
				triggered.clear()
			case _ =>
				cache.update(in1)
				context.timerService.registerProcessingTimeTimer(System.currentTimeMillis + instantDataWait)
				context.timerService.registerProcessingTimeTimer(System.currentTimeMillis + cacheTTL)
		}
	}

	override def processElement2(in2: SdcEnrichedHistorical, context: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#Context, collector: Collector[SdcCombined]): Unit = {
		cache.value() match {
			case i: SdcEnrichedInstant =>
				collector.collect(SdcCombined(i, in2))
				cache.clear()
				triggered.clear()
			case _ =>
				cache.update(in2)
				context.timerService.registerProcessingTimeTimer(System.currentTimeMillis + historicalDataWait)
				context.timerService.registerProcessingTimeTimer(System.currentTimeMillis + cacheTTL)
		}
	}

	override def onTimer(timestamp: Long, ctx: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#OnTimerContext, collector: Collector[SdcCombined]): Unit = {
		super.onTimer(timestamp, ctx, collector)
		(cache.value(), triggered.value()) match {
			case (_, true) =>
				cache.clear()
				triggered.clear()
			case (item: SdcEnrichedBase, false) =>
				collector.collect(SdcCombined(item))
				triggered.update(true)
			case _ =>
		}
	}

//	override def open(parameters: Configuration): Unit =  {
//		super.open(parameters)
//
//		val ttlConfig = StateTtlConfig
//				.newBuilder(Time.milliseconds(cacheTTL))
//				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//				.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
//				.cleanupFullSnapshot()
//				.build
//
//		val cacheDescriptor = new ValueStateDescriptor[SdcEnriched]("SDC_Combine", classOf[SdcEnriched])
//		cacheDescriptor.enableTimeToLive(ttlConfig)
//
//		cache = getRuntimeContext.getState(cacheDescriptor)
//	}
}
