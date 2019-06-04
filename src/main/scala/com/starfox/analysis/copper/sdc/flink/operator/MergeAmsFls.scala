package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, RichCoFlatMapFunction}
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */
//class MergeAmsFls2 extends RichCoFlatMapFunction[RawAms, RawFls, EnrichmentRecord] {
//	val FLSTTL: Integer = 2
//	val amsMappingDescriptor = new ValueStateDescriptor[RawAms]("AmsRaw", classOf[RawAms])
//	val flsMappingDescriptor = new ValueStateDescriptor[RawFls]("FlsRaw", classOf[RawFls])
//	val flsAgeDescriptor = new ValueStateDescriptor[Integer]("FlsAge", BasicTypeInfo.INT_TYPE_INFO)
//
//	override def open(parameters: Configuration): Unit = {
//		super.open(parameters)
//	}
//
//	override def flatMap1(in1: RawAms, out: Collector[EnrichmentRecord]): Unit = {
//		val cachedAms = getRuntimeContext.getState(amsMappingDescriptor)
//
//		if (cachedAms.value == null || cachedAms.value.ts <= in1.ts) {
//					cachedAms.update(in1)
//					val cachedFlsValue = getRuntimeContext.getState(flsMappingDescriptor).value
//					if (cachedFlsValue != null)
//						out.collect(EnrichmentRecord(chooseEnrichmentTime(cachedFlsValue.ts, in1.ts),
//							in1.dslam, in1.port, cachedFlsValue.data))
//		}
//	}
//
//	override def flatMap2(in2: RawFls, out: Collector[EnrichmentRecord]): Unit = {
//		val cachedFls = getRuntimeContext.getState(flsMappingDescriptor)
//
//		if (cachedFls.value == null || cachedFls.value.ts <= in2.ts) {
//			// update the States (including the TTL), and register a timer to handle TTL
//			cachedFls.update(in2)
//			getRuntimeContext.getState(flsAgeDescriptor).update(FLSTTL)
//			// send out a record if there's enough information
//			val cachedAmsValue = getRuntimeContext.getState(amsMappingDescriptor).value
//			if (cachedAmsValue != null)
//				out.collect(EnrichmentRecord(chooseEnrichmentTime(in2.ts, cachedAmsValue.ts),
//					cachedAmsValue.dslam, cachedAmsValue.port, in2.data))
//		}
//	}
//
//	private def chooseEnrichmentTime = (flsTime: Long, amsTime: Long) => Math.min(flsTime, amsTime)
//}

class MergeAmsFls extends CoProcessFunction[RawAms, RawFls, EnrichmentRecord] {
	val FLSTTL: Integer = 1
	val amsMappingDescriptor = new ValueStateDescriptor[RawAms]("AmsRaw", classOf[RawAms])
	val flsMappingDescriptor = new ValueStateDescriptor[RawFls]("FlsRaw", classOf[RawFls])
	val flsAgeDescriptor = new ValueStateDescriptor[Integer]("FlsAge", BasicTypeInfo.INT_TYPE_INFO)

	private def chooseEnrichmentTime = (flsTime: Long, amsTime: Long) => Math.min(flsTime, amsTime)

	override def processElement1(in1: RawAms, context: CoProcessFunction[RawAms, RawFls, EnrichmentRecord]#Context, collector: Collector[EnrichmentRecord]): Unit = {
		val cachedAms = getRuntimeContext.getState(amsMappingDescriptor)

		if (cachedAms.value == null || cachedAms.value.ts <= in1.ts) {
			cachedAms.update(in1)
			val cachedFlsValue = getRuntimeContext.getState(flsMappingDescriptor).value
			if (cachedFlsValue != null)
				collector.collect(EnrichmentRecord(chooseEnrichmentTime(cachedFlsValue.ts, in1.ts),
					in1.dslam, in1.port, cachedFlsValue.data))
		}
	}

	override def processElement2(in2: RawFls, context: CoProcessFunction[RawAms, RawFls, EnrichmentRecord]#Context, collector: Collector[EnrichmentRecord]): Unit = {
		val cachedFls = getRuntimeContext.getState(flsMappingDescriptor)

		if (cachedFls.value == null || cachedFls.value.ts <= in2.ts) {
			// update the States (including the TTL), and register a timer to handle TTL
			cachedFls.update(in2)
			getRuntimeContext.getState(flsAgeDescriptor).update(FLSTTL)
			context.timerService().registerEventTimeTimer(in2.ts + 1)
			// send out a record if there's enough information
			val cachedAmsValue = getRuntimeContext.getState(amsMappingDescriptor).value
			if (cachedAmsValue != null)
				collector.collect(EnrichmentRecord(chooseEnrichmentTime(in2.ts, cachedAmsValue.ts),
					cachedAmsValue.dslam, cachedAmsValue.port, in2.data))
		}
	}

	override def onTimer(timestamp: Long, ctx: CoProcessFunction[RawAms, RawFls, EnrichmentRecord]#OnTimerContext, out: Collector[EnrichmentRecord]): Unit = {
		super.onTimer(timestamp, ctx, out)
		val flsTtlState = getRuntimeContext.getState(flsAgeDescriptor)

		if (flsTtlState.value() == 0) {
			// This is the case when the state has expired
			getRuntimeContext.getState(flsMappingDescriptor).clear()
			val cachedAmsValue = getRuntimeContext.getState(amsMappingDescriptor).value
			if (cachedAmsValue != null)
				out.collect(EnrichmentRecord(timestamp, cachedAmsValue.dslam, cachedAmsValue.port, RawFls.WIPED_DATA))
		} else {
			// update the TTL, and register for another check
			flsTtlState.update(flsTtlState.value() - 1)
			ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1)
		}
	}
}
