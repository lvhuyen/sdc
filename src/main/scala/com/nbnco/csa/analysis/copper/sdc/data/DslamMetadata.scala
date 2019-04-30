package com.nbnco.csa.analysis.copper.sdc.data

import java.lang.{Long => JLong}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Created by Huyen on 19/10/18.
  */
case class DslamMetadata (columns: String,
						  relativePath: String,
						  fileTime: Long,
						  processingTime: Long,
						  componentFileTime: Long,
						  recordsCount: Int)
		extends IndexedRecord {

	override def getSchema: Schema = {
		DslamMetadata.getSchema
	}

	override def get(i: Int): AnyRef = {
		i match {
			case 0 => relativePath
			case 1 => fileTime: JLong
			case 2 => processingTime: JLong
			case 3 => componentFileTime: JLong
			case 4 => recordsCount: Integer
		}
	}

	override def put(i: Int, o: scala.Any): Unit = {
		throw new Exception("This class is for output only")
	}

}

object DslamMetadata {
	val EMPTY = new DslamMetadata("", "", 0L, 0L, 0L, 0)

	def apply(): DslamMetadata = EMPTY

	def apply(path: String): DslamMetadata = EMPTY.copy(relativePath = path)

	def apply(first: DslamMetadata, second: DslamMetadata, recordsCount: Int): DslamMetadata =
		EMPTY.copy(s"${first.columns},${second.columns}", recordsCount = recordsCount)

	def toMap(dslam: DslamMetadata): Map[String, Any] = {
		Map (
			"fileTime" -> dslam.fileTime,
			"processingTime" -> dslam.processingTime,
			"componentFileTime" -> dslam.componentFileTime,
			"recordsCount" -> dslam.recordsCount,
			"path" -> dslam.relativePath
		)
	}

	def getSchema: Schema = {
		org.apache.avro.SchemaBuilder
			.record("DslamMetaData").namespace("com.nbnco")
			.fields()
			.name("relativePath").`type`("string").noDefault()
			.name("fileTime").`type`("long").noDefault()
			.name("processingTime").`type`("long").noDefault()
			.name("componentFileTime").`type`("long").noDefault()
			.name("recordsCount").`type`("int").noDefault()
			.endRecord()
	}
}
