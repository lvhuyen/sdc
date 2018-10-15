package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/7/18.
  */
case class FlsRecord(ts: Long, dslam: String, port: String, avcId: String, cpi: String)
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

object FlsRecord {
	def apply(): FlsRecord = FlsRecord (0L, "", "", "", "")
}
