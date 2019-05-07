package com.starfox.analysis.copper.sdc.data

/**
  * Created by Huyen on 15/8/18.
  */

trait SdcRawBase extends CopperLine {
	def enrich(enrich: EnrichmentData): SdcEnrichedBase
}
