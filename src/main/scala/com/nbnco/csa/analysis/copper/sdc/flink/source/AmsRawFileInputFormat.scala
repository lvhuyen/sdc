package com.nbnco.csa.analysis.copper.sdc.flink.source

import com.nbnco.csa.analysis.copper.sdc.data.AmsRaw
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.parquet.ParquetPojoInputFormat
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 9/9/18.
  */
object AmsRawFileInputFormat {
	private val LOG = LoggerFactory.getLogger(classOf[AmsRawFileInputFormat])
}

class AmsRawFileInputFormat(filePath: Path)
		extends ParquetPojoInputFormat[AmsRaw](filePath, createTypeInformation[AmsRaw].asInstanceOf[PojoTypeInfo[AmsRaw]]){
	override def open(split: FileInputSplit): Unit = {
		try {
			super.open(split)
		} catch {
			case _: java.io.IOException =>
				AmsRawFileInputFormat.LOG.warn("Error opening file : {}", split.getPath.getPath)
			case _: java.lang.RuntimeException =>
				AmsRawFileInputFormat.LOG.warn("File with invalid format: {}", split.getPath.getPath)
			case a: Throwable => throw a
		}
	}
}

