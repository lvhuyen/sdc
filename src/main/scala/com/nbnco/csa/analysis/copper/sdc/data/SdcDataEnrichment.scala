package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataEnrichment(tsEnrich: Long, avcId: String, cpi: String) {
	def toMap: Map[String, Any] = {
		Map (
			"enrich_timestamp" -> tsEnrich,
			"avcid" -> avcId,
			"cpid" -> cpi
		)
	}
}

object SdcDataEnrichment {
	def apply(): SdcDataEnrichment = SdcDataEnrichment(0L, null, null)
}