package com.nbnco.csa.analysis.copper.sdc.flink.source

import java.util.{Date, TimeZone}

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.core.fs.Path
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by Huyen on 9/9/18.
  */

object SdcFilePathFilter {
	private val LOG = LoggerFactory.getLogger(classOf[SdcFilePathFilter])
}

class SdcFilePathFilter(lookBackPeriod: Long) extends FilePathFilter {
	private val FILENAME_PATTERN = """.*(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}).tar.gz$""".r
	private val FILENAME_DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm")
	FILENAME_DATE_FORMAT.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

	private val DIRNAME_PATTERN = """^(\d{8})$""".r
	private val DIRNAME_DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMdd")
	DIRNAME_DATE_FORMAT.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

	val lookBackPeriodWithMargin = lookBackPeriod + 86400000L

	/**
	  * Ignore this file if it ends with tar.gz and in the right format and is older than `lookBackPeriod` from now
	 */
	override def filterPath(filePath: Path): Boolean = {
		filePath == null ||
				filePath.getName.startsWith(".") ||
				filePath.getName.startsWith("_") ||
				filePath.getName.contains(FilePathFilter.HADOOP_COPYING) ||
				Try(DIRNAME_PATTERN.findFirstMatchIn(filePath.getName)
						.exists(r => DIRNAME_DATE_FORMAT.parse(r.group(1)).getTime < new Date().getTime - lookBackPeriodWithMargin))
						.toOption.getOrElse(false) ||
				Try(FILENAME_PATTERN.findFirstMatchIn(filePath.getName)
						.exists(r => FILENAME_DATE_FORMAT.parse(r.group(1)).getTime < new Date().getTime - lookBackPeriod))
						.toOption.getOrElse(false)
	}
}
