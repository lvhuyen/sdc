package com.nbnco.csa.analysis.copper.sdc.data

import java.lang.{Long => JLong}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Created by Huyen on 1/10/18.
  */
case class DslamRaw[DataType](ts: Long,
							  name: String,
							  dslamType: Int,
							  data: DataType,
							  metadata: DslamMetadata)
		extends IndexedRecord with TemporalEvent {

	override def get(i: Int): AnyRef = i match {
		case 0 => ts: JLong
		case 1 => name
		case 2 => dslamType: Integer
		case 3 => metadata.relativePath
		case 4 => metadata.fileTime: JLong
		case 5 => metadata.processingTime: JLong
		case 6 => metadata.componentFileTime: JLong
		case 7 => metadata.recordsCount: Integer

	}

	override def put(i: Int, o: scala.Any): Unit = {
		throw new Exception("This class is for output only")
	}

	override def getSchema: Schema = {
		DslamRaw.SCHEMA
	}
}

object DslamRaw {
	val SCHEMA: Schema = {
		org.apache.avro.SchemaBuilder
				.record("DslamMetaData").namespace("com.nbnco")
				.fields()
				.name("metricsTime").`type`("long").noDefault()
				.name("name").`type`("string").noDefault()
				.name("type").`type`("int").noDefault()
				.name("relativePath").`type`("string").noDefault()
				.name("fileTime").`type`("long").noDefault()
				.name("processingTime").`type`("long").noDefault()
				.name("componentFileTime").`type`("long").noDefault()
				.name("recordsCount").`type`("int").noDefault()
				.endRecord()
	}

	def toMap(dslam: DslamRaw[None.type]): Map[String, Any] = {
		Map (
			"type" -> dslam.dslamType,
			"dslam" -> dslam.name,
			"metricsTime" -> dslam.ts,
			"fileTime" -> dslam.metadata.fileTime,
			"processingTime" -> dslam.metadata.processingTime,
			"componentFileTime" -> dslam.metadata.componentFileTime,
			"recordsCount" -> dslam.metadata.recordsCount,
			"path" -> dslam.metadata.relativePath
		)
	}
}