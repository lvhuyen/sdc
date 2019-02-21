package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */
class CalculatePercentiles(enrichedI: OutputTag[SdcEnrichedInstant], enrichedH: OutputTag[SdcEnrichedHistorical])
		extends CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase] {

	val mappingDescriptor = new ValueStateDescriptor[EnrichmentData](
			"Enrichment_Mapping", classOf[EnrichmentData])

	@ForwardedFields(Array("dslam","port"))
	override def processElement1(in1: SdcRawBase, context: CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase]#Context, out: Collector[SdcEnrichedBase]): Unit = {
		val mapping = getRuntimeContext.getState(mappingDescriptor).value

		val result = if (mapping != null) in1.enrich(mapping) else in1.enrich(Map.empty)
		out.collect(result)
		result match {
			case i: SdcEnrichedInstant =>
				context.output(enrichedI, i)
			case h: SdcEnrichedHistorical =>
				context.output(enrichedH, h)
		}
	}

	override def processElement2(in2: EnrichmentRecord, context: CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase]#Context, out: Collector[SdcEnrichedBase]): Unit = {
		val mapping = getRuntimeContext.getState(mappingDescriptor)
		mapping.update(
			if (mapping.value == null) in2.data
			else in2.data ++ mapping.value())
	}
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
