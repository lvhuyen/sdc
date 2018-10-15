package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 15/8/18.
  */

trait SdcRawBase extends SdcRecord {
	def enrich(enrich: SdcDataEnrichment): SdcEnrichedBase
}
