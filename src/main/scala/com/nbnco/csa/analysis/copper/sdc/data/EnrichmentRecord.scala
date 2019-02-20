package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/7/18.
  */
case class EnrichmentRecord(ts: Long, dslam: String, port: String, avcId: String, cpi: String)
		extends SdcRecord {

	def this() = this (0L, "", "", "", "")

	override def toMap: Map[String, Any] = {
		Map(
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts,
			"avc" -> avcId,
			"cpi" -> cpi
		)
	}
}

object EnrichmentRecord {
	def apply(): EnrichmentRecord = EnrichmentRecord (0L, "", "", "", "")
}
