package com.starfox.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataEnrichment(ts: Long,
							 avc: String,
							 cpi: String,
							 corrAttndrDs: Int,
							 corrAttndrUs: Int) {
	def toMap: Map[String, Any] = {
		Map (
			"enrich_timestamp" -> System.currentTimeMillis(),
			"avcid" -> avc,
			"cpid" -> cpi,
			"correctedAttainableNetDataRateDownstream" -> corrAttndrDs,
			"correctedAttainableNetDataRateUpstream" -> corrAttndrUs
		)
	}
}

object SdcDataEnrichment {
	val EMPTY = SdcDataEnrichment(0L, null, null, 0, 0)

	def apply(): SdcDataEnrichment = EMPTY
}