package com.starfox.analysis.copper.sdc.flink.source

import com.starfox.analysis.copper.sdc.data.PojoFls
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.parquet.ParquetPojoInputFormat
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 9/9/18.
  */
class FlsParquetFileInputFormat(filePath: Path)
		extends ParquetPojoInputFormat[PojoFls](filePath, createTypeInformation[PojoFls].asInstanceOf[PojoTypeInfo[PojoFls]]){
	override def open(split: FileInputSplit): Unit = {
		try {
			super.open(split)
		} catch {
			case _: java.io.IOException =>
				FlsParquetFileInputFormat.LOG.warn("Error opening file: {}", split.getPath.getPath)
			case _: java.lang.RuntimeException =>
				FlsParquetFileInputFormat.LOG.warn("File with invalid format: {}", split.getPath.getPath)
			case a: Throwable => throw a
		}
	}
}

object FlsParquetFileInputFormat {
	private val LOG = LoggerFactory.getLogger(classOf[FlsParquetFileInputFormat])
}
