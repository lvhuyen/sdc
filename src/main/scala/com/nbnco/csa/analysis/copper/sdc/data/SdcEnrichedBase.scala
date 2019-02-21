package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 15/8/18.
  */
trait SdcEnrichedBase extends CopperLine {

	val enrich: SdcDataEnrichment

	override def toMap: Map[String, Any]
}


