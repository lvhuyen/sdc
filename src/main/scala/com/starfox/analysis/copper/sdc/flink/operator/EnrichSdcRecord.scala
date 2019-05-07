package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data._

import java.lang.{Float => JFloat}

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector
/**
  * Created by Huyen on 15/8/18.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */

object EnrichSdcRecord {
	def apply(streamSdcCombinedRaw: DataStream[SdcCombined],
			  streamPctls: DataStream[(String, List[JFloat])],
			  streamEnrichmentAgg: DataStream[EnrichmentRecord]): DataStream[SdcCombined] = {

		val pctlsStateDescriptor = new MapStateDescriptor(
			"PercentilessBroadcastState",
			BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO);
		val streamPctlsBroadcast = streamPctls.broadcast(pctlsStateDescriptor)

		streamSdcCombinedRaw.map(_.asInstanceOf[CopperLine])
				.union(streamEnrichmentAgg.map(_.asInstanceOf[CopperLine]))
				.keyBy(r => (r.dslam, r.port))
				.connect(streamPctlsBroadcast)
				.process(new EnrichSdcRecord())
				.uid(OperatorId.SDC_ENRICHER)
				.name("Enrich SDC records")
	}
}

class EnrichSdcRecord extends KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JFloat]), SdcCombined] {

	val enrichmentStateDescriptor = new ValueStateDescriptor[EnrichmentData](
		"Enrichment_Mapping", classOf[EnrichmentData])

	val pctlsStateDescriptor = new MapStateDescriptor(
		"PercentilessBroadcastState",
		BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO)

	override def processBroadcastElement(in2: (String, List[JFloat]), context: KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JFloat]), SdcCombined]#Context, collector: Collector[SdcCombined]): Unit = {
		context.getBroadcastState(pctlsStateDescriptor).put(in2._1, in2._2.toArray)
	}

	override def processElement(in1: CopperLine, readOnlyContext: KeyedBroadcastProcessFunction[(String, String), CopperLine, (String, List[JFloat]), SdcCombined]#ReadOnlyContext, collector: Collector[SdcCombined]): Unit = {
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

			case i: SdcCombined =>
				collector.collect(
					if (cachedEnrichmentData != null) {
						val techType = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TECH_TYPE, TechType.NotSupported).asInstanceOf[TechType.TechType]
						val atten365 = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.ATTEN365, -1: Short).asInstanceOf[Short]
						val dpboProfile = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.DPBO_PROFILE, -1: Byte).asInstanceOf[Byte]
						val tier_ds = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TC4_DS, "").asInstanceOf[String]
						val tier_us = cachedEnrichmentData.getOrElse(EnrichmentAttributeName.TC4_US, "").asInstanceOf[String]
						val (c_ds, c_us, e_ds, e_us) = CalculatePercentiles.buildKeysForGettingPctlsTable(techType, atten365, tier_ds, tier_us)

						val corrected_ds = CalculatePercentiles.calculate_corrected_attndr(techType, atten365,
							cachedEnrichmentData.getOrElse(EnrichmentAttributeName.NOISE_MARGIN_DS, -1: Short).asInstanceOf[Short],
							tier_ds, i.dataI.attndrDs, dpboProfile, cachedPctls.get(c_ds), cachedPctls.get(e_ds), false)
						val corrected_us = CalculatePercentiles.calculate_corrected_attndr(techType, atten365,
							cachedEnrichmentData.getOrElse(EnrichmentAttributeName.NOISE_MARGIN_US, -1: Short).asInstanceOf[Short],
							tier_us, i.dataI.attndrUs, dpboProfile, cachedPctls.get(c_us), cachedPctls.get(e_us), false)


						SdcCombined(i, cachedEnrichmentData, corrected_ds, corrected_us)
					}
					else
						i
				)
		}
	}
}
