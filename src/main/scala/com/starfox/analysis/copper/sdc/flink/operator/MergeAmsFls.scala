package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */
class MergeAmsFls extends RichCoFlatMapFunction[RawAms, RawFls, EnrichmentRecord] {
	val amsMappingDescriptor = new ValueStateDescriptor[RawAms]("AmsRaw", classOf[RawAms])
	val flsMappingDescriptor = new ValueStateDescriptor[RawFls]("FlsRaw", classOf[RawFls])

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
	}

	override def flatMap1(in1: RawAms, out: Collector[EnrichmentRecord]): Unit = {
		val cachedAms = getRuntimeContext.getState(amsMappingDescriptor)

		if (cachedAms.value == null || cachedAms.value.ts < in1.ts) {
					cachedAms.update(in1)
					val cachedFlsValue = getRuntimeContext.getState(flsMappingDescriptor).value
					if (cachedFlsValue != null)
						out.collect(EnrichmentRecord(chooseEnrichmentTime(cachedFlsValue.ts, in1.ts),
							in1.dslam, in1.port, cachedFlsValue.data))
		}
	}

	override def flatMap2(in2: RawFls, out: Collector[EnrichmentRecord]): Unit = {
		val cachedFls = getRuntimeContext.getState(flsMappingDescriptor)

		if (cachedFls.value == null || cachedFls.value.ts < in2.ts) {
			cachedFls.update(in2)
			val cachedAmsValue = getRuntimeContext.getState(amsMappingDescriptor).value
			if (cachedAmsValue != null)
				out.collect(EnrichmentRecord(chooseEnrichmentTime(in2.ts, cachedAmsValue.ts),
					cachedAmsValue.dslam, cachedAmsValue.port, in2.data))
		}
	}

	private def chooseEnrichmentTime = (flsTime: Long, amsTime: Long) => Math.min(flsTime, amsTime)
}

//class EnrichSdcRecord extends CoProcessFunction[SdcRaw, FlsRecord, SdcEnriched] {
//	val mappingDescriptor = new ValueStateDescriptor[(String, String, Long)]("mapping", classOf[(String, String, Long)])
//
//	override def processElement1(in1: SdcRaw, context: CoProcessFunction[SdcRaw, FlsRecord, SdcEnriched]#Context, out: Collector[SdcEnriched]): Unit = {
//		val mapping = getRuntimeContext.getState(mappingDescriptor).value
//		val (avc_id, cpi, enrichment_date) =
//			if (mapping != null) (Some(mapping._1), Some(mapping._2), mapping._3)
//			else (None, None, -1L)
//
//		out.collect(in1.enrich(avc_id, cpi, enrichment_date))
//	}
//
//	override def processElement2(in2: FlsRecord, context: CoProcessFunction[SdcRaw, FlsRecord, SdcEnriched]#Context, collector: Collector[SdcEnriched]): Unit = {
//		val mapping = getRuntimeContext.getState(mappingDescriptor)
//		if (mapping.value == null || mapping.value._3 < in2.metrics_date)
//			mapping.update((in2.avc_id, in2.cpi, in2.metrics_date))
//	}
//}
