package com.starfox.analysis.copper.sdc.data

/**
  * Created by Huyen on 15/8/18.
  */
trait SdcEnrichedBase extends CopperLine {
	override def toMap: Map[String, Any]
}


