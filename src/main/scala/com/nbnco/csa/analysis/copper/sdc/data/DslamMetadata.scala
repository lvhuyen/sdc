package com.nbnco.csa.analysis.copper.sdc.data

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Created by Huyen on 19/10/18.
  */
case class DslamMetadata (isInstant: Boolean, name: String, ts: Long, columns: String, relativePath: String, fileTime: Long, processingTime: Long)
	extends IndexedRecord with TemporalEvent {
	override def getSchema: Schema = {
		DslamMetadata.getSchema()
	}
	override def get(i: Int): AnyRef = {
		i match {
			case 0 => isInstant.asInstanceOf[AnyRef]
			case 1 => name
			case 2 => ts.asInstanceOf[AnyRef]
			case 3 => relativePath
			case 4 => fileTime.asInstanceOf[AnyRef]
			case 5 => processingTime.asInstanceOf[AnyRef]
		}
	}

	override def put(i: Int, o: scala.Any): Unit = {
		throw new Exception("This class is for output only")
	}

}

object DslamMetadata {
	def toMap(dslam: DslamMetadata): Map[String, Any] = {
		Map (
			"isInstant" -> dslam.isInstant,
			"dslam" -> dslam.name,
			"metricsTime" -> dslam.ts,
			"fileTime" -> dslam.fileTime,
			"processingTime" -> dslam.processingTime,
			"path" -> dslam.relativePath
		)
	}

	def getSchema(): Schema = {
		org.apache.avro.SchemaBuilder
			.record("DslamMetaData").namespace("com.nbnco")
			.fields()
			.name("isInstant").`type`("boolean").noDefault()
			.name("name").`type`("string").noDefault()
			.name("metricsTime").`type`("long").noDefault()
			.name("relativePath").`type`("string").noDefault()
			.name("fileTime").`type`("long").noDefault()
			.name("processingTime").`type`("long").noDefault()
			.endRecord()
	}
}
