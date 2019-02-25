package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataEnrichment(tsEnrich: Long, avcId: String, cpi: String) {
	def toMap: Map[String, Any] = {
		Map (
			"enrich_timestamp" -> System.currentTimeMillis(),
			"avc_id" -> avcId,
			"cpi" -> cpi
		)
	}
}

object SdcDataEnrichment {
	val BLANK = SdcDataEnrichment(0L, null, null)

	def apply(): SdcDataEnrichment = BLANK
}