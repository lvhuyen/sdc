package com.nbnco.csa.analysis.copper.sdc.flink.source

import com.nbnco.csa.analysis.copper.sdc.data.FlsRaw
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.parquet.ParquetPojoInputFormat
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 9/9/18.
  */
class FlsRawFileInputFormat(filePath: Path)
		extends ParquetPojoInputFormat[FlsRaw](filePath, createTypeInformation[FlsRaw].asInstanceOf[PojoTypeInfo[FlsRaw]]){
	override def open(split: FileInputSplit): Unit = {
		try {
			super.open(split)
		} catch {
			case _: java.io.IOException =>
				FlsRawFileInputFormat.LOG.warn("Error opening file: {}", split.getPath.getPath)
			case _: java.lang.RuntimeException =>
				FlsRawFileInputFormat.LOG.warn("File with invalid format: {}", split.getPath.getPath)
			case a: Throwable => throw a
		}
	}
}

object FlsRawFileInputFormat {
	private val LOG = LoggerFactory.getLogger(classOf[FlsRawFileInputFormat])
}
