package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data.{AmsRaw, FlsRaw, FlsRecord}
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
class MergeAmsFls extends RichCoFlatMapFunction[AmsRaw, FlsRaw, FlsRecord] {
	val amsMappingDescriptor = new ValueStateDescriptor[(String, String, Long)]("AmsRaw", classOf[(String, String, Long)])
	val flsMappingDescriptor = new ValueStateDescriptor[(String, String, Long)]("FlsRaw", classOf[(String, String, Long)])

	//	@transient private var nRecords: Meter = _

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)

		//		nRecords = getRuntimeContext
		//				.getMetricGroup
		//				.addGroup("Chronos")
		//				.meter("NEED_A_NAME", new org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper(
		//					new com.codahale.metrics.Meter()))
	}

	override def flatMap1(in1: AmsRaw, out: Collector[FlsRecord]): Unit = {
		val cachedAms = getRuntimeContext.getState(amsMappingDescriptor)

		val current_ts = in1.metrics_date.toEpochMilli
		if (cachedAms.value == null || cachedAms.value._3 < current_ts) {
			in1 match {
				case AmsRaw((dslam: String, port: String)) =>
					cachedAms.update((dslam, port, current_ts))
					val cachedFlsValue = getRuntimeContext.getState(flsMappingDescriptor).value
					if (cachedFlsValue != null)
						out.collect(FlsRecord(enrichment_time(cachedFlsValue._3, current_ts),
							dslam, port, cachedFlsValue._1, cachedFlsValue._2))
			}
		}
	}

	override def flatMap2(in2: FlsRaw, out: Collector[FlsRecord]): Unit = {
		val cachedFls = getRuntimeContext.getState(flsMappingDescriptor)

		val current_ts = in2.metrics_date.toEpochMilli
		if (cachedFls.value == null || cachedFls.value._3 < current_ts) {
			cachedFls.update((in2.avc_id, in2.ntd_id, in2.metrics_date.toEpochMilli))
			val cachedAmsValue = getRuntimeContext.getState(amsMappingDescriptor).value
			if (cachedAmsValue != null)
				out.collect(FlsRecord(enrichment_time(current_ts, cachedAmsValue._3),
					cachedAmsValue._1, cachedAmsValue._2, in2.avc_id, in2.ntd_id))
		}
	}

	private def enrichment_time = (flsTime: Long, amsTime: Long) => Math.min(flsTime, amsTime)
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
