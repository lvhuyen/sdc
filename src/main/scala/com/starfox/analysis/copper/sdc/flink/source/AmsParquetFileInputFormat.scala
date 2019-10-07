package com.starfox.analysis.copper.sdc.flink.source

import com.starfox.analysis.copper.sdc.data.PojoAms
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.{FileInputSplit, Path}
import com.starfox.flink.source.ParquetPojoInputFormat
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 9/9/18.
  */
object AmsParquetFileInputFormat {
	private val LOG = LoggerFactory.getLogger(classOf[AmsParquetFileInputFormat])
}

class AmsParquetFileInputFormat(filePath: Path)
		extends ParquetPojoInputFormat[PojoAms](filePath,
			createTypeInformation[PojoAms].asInstanceOf[PojoTypeInfo[PojoAms]]){
	override def open(split: FileInputSplit): Unit = {
		try {
			super.open(split)
		} catch {
			case _: java.io.IOException =>
				AmsParquetFileInputFormat.LOG.warn("Error opening file : {}", split.getPath.getPath)
			case _: java.lang.RuntimeException =>
				AmsParquetFileInputFormat.LOG.warn("File with invalid format: {}", split.getPath.getPath)
			case a: Throwable => throw a
		}
	}
}

