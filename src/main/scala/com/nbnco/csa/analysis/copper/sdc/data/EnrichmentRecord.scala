package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/7/18.
  */

import com.nbnco.csa.analysis.copper.sdc.data.EnrichmentAttributeName._
import org.slf4j.LoggerFactory
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
	private val LOG = LoggerFactory.getLogger(classOf[EnrichmentRecord])
	val UNKNOWN = EnrichmentRecord (0L, "", "", Map.empty)

	private val regexFeatureSetPort = """(\d+)-(\d+)-(\d+)-(\d+)""".r

	def apply(): EnrichmentRecord = UNKNOWN

	def apply(raw: PojoNac): EnrichmentRecord = {
		try {
			val (nm_ds, nm_us) =
				if (raw.max_additional_noise_margin_ds == null ||
						raw.max_additional_noise_margin_ds.isEmpty ||
						raw.max_additional_noise_margin_us == null ||
						raw.max_additional_noise_margin_us.isEmpty)
					(-1: Short, -1: Short)
				else
					(raw.max_additional_noise_margin_ds.toFloat.toShort, raw.max_additional_noise_margin_us.toFloat.toShort)

			EnrichmentRecord(Long.MinValue,
				raw.dslam,
				regexFeatureSetPort.replaceFirstIn(raw.port, "R$1.S$2.LT$3.P$4"),
				Map(
					EnrichmentAttributeName.NOISE_MARGIN_DS -> nm_ds,
					EnrichmentAttributeName.NOISE_MARGIN_US -> nm_us,
					EnrichmentAttributeName.DPBO_PROFILE -> {
						if (raw.dpbo_profile_name.equals("ADP-VDSL_00dB")) 0: Byte else 1: Byte
					}
				))
		} catch {
			case _: Throwable =>
				LOG.warn("Bad NAC record: {}", raw)
				UNKNOWN
		}
	}

	def apply(raw: PojoChronosFeatureSetFloat): EnrichmentRecord = {
		try {
			val (dslam, port) = raw.id.span(_ != ':')
			EnrichmentRecord(Long.MinValue, dslam, regexFeatureSetPort.replaceFirstIn(port.drop(1), "R$1.S$2.LT$3.P$4"),
				Map(EnrichmentAttributeName.ATTEN365 -> raw.value))
		} catch {
			case _: Throwable =>
				LOG.warn("Bad Atten375 record: {}", raw)
				UNKNOWN
		}
	}
}
