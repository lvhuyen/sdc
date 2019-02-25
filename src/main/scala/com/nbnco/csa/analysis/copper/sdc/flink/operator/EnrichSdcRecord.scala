package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo}
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.{Collector, OutputTag}

/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */

class EnrichSdcRecord (enrichedI: OutputTag[SdcEnrichedInstant], enrichedH: OutputTag[SdcEnrichedHistorical])
		extends KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JavaFloat]), SdcEnrichedBase] {

	val enrichmentStateDescriptor = new ValueStateDescriptor[EnrichmentData](
		"Enrichment_Mapping", classOf[EnrichmentData])

	val pctlsStateDescriptor = new MapStateDescriptor(
		"PercentilessBroadcastState",
		BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO)

	override def processBroadcastElement(in2: (String, List[JavaFloat]), context: KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JavaFloat]), SdcEnrichedBase]#Context, collector: Collector[SdcEnrichedBase]): Unit = {
		context.getBroadcastState(pctlsStateDescriptor).put(in2._1, in2._2.toArray)
	}

	override def processElement(in1: CopperLine, readOnlyContext: KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JavaFloat]), SdcEnrichedBase]#ReadOnlyContext, collector: Collector[SdcEnrichedBase]): Unit = {
		val cachedEnrichmentStateDesc = getRuntimeContext.getState(enrichmentStateDescriptor)
		val cachedEnrichmentData = cachedEnrichmentStateDesc.value()
		val cachedPctls = readOnlyContext.getBroadcastState(pctlsStateDescriptor)
		in1 match {
			case e: EnrichmentRecord =>
				// Change the atten365 from Float to Short
				val new_data = e.data.get(EnrichmentAttributeName.ATTEN365) match {
					case Some(atten: Float) =>
						e.data.updated(EnrichmentAttributeName.ATTEN365, CalculatePercentiles.getAttenIndex(atten))
					case _ => e.data
				}
				// Add new enrichment data to the Cache
				cachedEnrichmentStateDesc.update(
					if (cachedEnrichmentData == null)
						new_data
					else
						cachedEnrichmentData ++ new_data
				)

			case h: SdcRawHistorical =>
				val result:SdcEnrichedHistorical = if (cachedEnrichmentData != null)
					h.enrich(cachedEnrichmentData)
				else
					h.enrich(Map.empty)
				collector.collect(result)
				readOnlyContext.output(enrichedH, result)

			case i: SdcRawInstant =>
				val result =
					if (cachedEnrichmentData != null) {
						val techType = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TECH_TYPE, TechType.NotSupported).asInstanceOf[TechType.TechType]
						val atten365 = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.ATTEN365, (-1).toShort).asInstanceOf[Short]
						val dpboProfile = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.DPBO_PROFILE, (-1).toByte).asInstanceOf[Byte]
						val tier_ds = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TC4_DS, "").asInstanceOf[String]
						val tier_us = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TC4_US, "").asInstanceOf[String]
						val (c_ds, c_us, e_ds, e_us) = CalculatePercentiles.buildKeysForGettingPctlsTable(techType, atten365, tier_ds, tier_us)

						val corrected_ds = CalculatePercentiles.calculate_corrected_attndr(techType, atten365,
							cachedEnrichmentData.getOrElse(EnrichmentAttributeName.NOISE_MARGIN_DS, (-1).toShort).asInstanceOf[Short],
							tier_ds, i.data.attndr_ds, dpboProfile, cachedPctls.get(c_ds), cachedPctls.get(e_ds), false)
						val corrected_us = CalculatePercentiles.calculate_corrected_attndr(techType, atten365,
							cachedEnrichmentData.getOrElse(EnrichmentAttributeName.NOISE_MARGIN_US, (-1).toShort).asInstanceOf[Short],
							tier_us, i.data.attndr_us, dpboProfile, cachedPctls.get(c_us), cachedPctls.get(e_us), false)
						SdcEnrichedInstant(i, cachedEnrichmentData, corrected_ds, corrected_us)
					}
					else
						SdcEnrichedInstant(i)
				collector.collect(result)
				readOnlyContext.output(enrichedI, result)
		}
	}
}

//class EnrichSdcRecord(enrichedI: OutputTag[SdcEnrichedInstant], enrichedH: OutputTag[SdcEnrichedHistorical])
//		extends CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase] {
//
//	val mappingDescriptor = new ValueStateDescriptor[EnrichmentData](
//			"Enrichment_Mapping", classOf[EnrichmentData])
//
//	@ForwardedFields(Array("dslam","port"))
//	override def processElement1(in1: SdcRawBase, context: CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase]#Context, out: Collector[SdcEnrichedBase]): Unit = {
//		val mapping = getRuntimeContext.getState(mappingDescriptor).value
//
//		val result = if (mapping != null) in1.enrich(mapping) else in1.enrich(Map.empty)
//		out.collect(result)
//		result match {
//			case i: SdcEnrichedInstant =>
//				context.output(enrichedI, i)
//			case h: SdcEnrichedHistorical =>
//				context.output(enrichedH, h)
//		}
//	}
//
//	override def processElement2(in2: EnrichmentRecord, context: CoProcessFunction[SdcRawBase, EnrichmentRecord, SdcEnrichedBase]#Context, out: Collector[SdcEnrichedBase]): Unit = {
//		val mapping = getRuntimeContext.getState(mappingDescriptor)
//		mapping.update(
//			if (mapping.value == null) in2.data
//			else in2.data ++ mapping.value())
//	}
//}

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
