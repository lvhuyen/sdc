package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/7/18.
  */

import com.nbnco.csa.analysis.copper.sdc.data.EnrichmentAttributeName._
case class EnrichmentRecord(ts: Long, dslam: String, port: String, data: EnrichmentData)
		extends CopperLine {

	def this() = this (0L, "", "", Map.empty)

	override def toMap: Map[String, Any] = {
		Map(
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts,
			"avc" -> data.getOrElse(AVC, ""),
			"cpi" -> data.getOrElse(CPI, "")
		)
	}
}

object EnrichmentRecord {
	def apply(): EnrichmentRecord = EnrichmentRecord (0L, "", "", Map.empty)

	def apply(raw: PojoNac): EnrichmentRecord =
		EnrichmentRecord(Long.MinValue, raw.dslam, raw.port, Map(
			EnrichmentAttributeName.NOISE_MARGIN_DS -> raw.max_additional_noise_margin_ds.toInt,
			EnrichmentAttributeName.NOISE_MARGIN_US -> raw.max_additional_noise_margin_us.toInt,
			EnrichmentAttributeName.DPBO_PROFILE -> {if (raw.dpbo_profile_name.equals("ADP-VDSL_00dB")) 0 else 1}
		))

	def apply(raw: PojoChronosFeatureSetFloat): EnrichmentRecord = {
		val (dslam, port) = raw.id.span(_ != ':')
		EnrichmentRecord(Long.MinValue, dslam, port.drop(1), Map(EnrichmentAttributeName.ATTEN365 -> raw.value))
	}
}
