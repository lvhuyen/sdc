package com.nbnco.csa.analysis.copper.sdc.flink.source

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.parquet.ParquetPojoInputFormat
import org.slf4j.LoggerFactory

class ChronosParquetFileInputFormat[PojoType](filePath: Path, typeInfo: TypeInformation[PojoType])
		extends ParquetPojoInputFormat[PojoType](filePath, typeInfo.asInstanceOf[PojoTypeInfo[PojoType]]){
	override def open(split: FileInputSplit): Unit = {
		try {
			super.open(split)
		} catch {
			case _: java.io.IOException =>
				ChronosParquetFileInputFormat.LOG.warn("Error opening file: {}", split.getPath.getPath)
				super.close()
			case _: java.lang.RuntimeException =>
				ChronosParquetFileInputFormat.LOG.warn("File with invalid format: {}", split.getPath.getPath)
				super.close()
			case a: Throwable => throw a
		}
	}

//	override
}

object ChronosParquetFileInputFormat {
	private val LOG = LoggerFactory.getLogger(classOf[ChronosParquetFileInputFormat[Any]])
}