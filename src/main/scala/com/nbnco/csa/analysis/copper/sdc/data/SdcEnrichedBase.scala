package com.nbnco.csa.analysis.copper.sdc.data

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/**
  * Created by Huyen on 15/8/18.
  */
trait SdcEnrichedBase extends SdcRecord {

	val enrich: SdcDataEnrichment

//	override def toString:String = {
//		implicit val formats = DefaultFormats
//		write(this)
//	}

//	def getStaticSchema(): Schema

	override def toMap: Map[String, Any]
}


