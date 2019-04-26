package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataEnrichment(tsEnrich: Long, avcId: String, cpi: String, corrAttndrDs: Integer, corrAttndrUs: Integer) {
	def toMap: Map[String, Any] = {
		Map (
			"enrich_timestamp" -> System.currentTimeMillis(),
			"avc_id" -> avcId,
			"cpi" -> cpi,
			"correctedAttndrDs" -> corrAttndrDs,
			"correctedAttndrUs" -> corrAttndrUs
		)
	}
}

object SdcDataEnrichment {
	val EMPTY = SdcDataEnrichment(0L, null, null, 0, 0)

	def apply(): SdcDataEnrichment = EMPTY
}