package com.nbnco.csa.analysis.copper.sdc.flink.source

import java.text.SimpleDateFormat
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit, TemporalUnit}
import java.util.{Date, TimeZone}

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.core.fs.Path
import org.slf4j.{Logger, LoggerFactory}

object EnrichmentFilePathFilter{
	private val FOLDER_SIGNATURE = "dt="
	private val FOLDER_SIGNATURE_LENGTH = FOLDER_SIGNATURE.length
	private val LOG = LoggerFactory.getLogger(classOf[EnrichmentFilePathFilter])
}

class EnrichmentFilePathFilter(lookBackDays: Long, datePattern: String, dateRegex: String) extends FilePathFilter {
	private val DATE_FORMAT = new java.text.SimpleDateFormat(datePattern)
	DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

	val lookBackMillis = lookBackDays * 86400 * 1000
	val folerRegex = s"^${EnrichmentFilePathFilter.FOLDER_SIGNATURE}${dateRegex}$$"

	override def filterPath(filePath: Path): Boolean = {
		filePath == null ||
				filePath.getName.startsWith(".") ||
				filePath.getName.startsWith("_") ||
				filePath.getName.contains(FilePathFilter.HADOOP_COPYING) ||
				{
					try {
						val fileStatus = filePath.getFileSystem.getFileStatus(filePath)
						if (!fileStatus.isDir) {
							fileStatus.getLen == 0
						} else
							filePath.getName.startsWith(EnrichmentFilePathFilter.FOLDER_SIGNATURE) &&
									filePath.getName.matches(s"^${EnrichmentFilePathFilter.FOLDER_SIGNATURE}${dateRegex}$$") &&
									this.DATE_FORMAT.parse(
										filePath.getName.drop(EnrichmentFilePathFilter.FOLDER_SIGNATURE_LENGTH)).getTime <
											LocalDate.now(ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli - lookBackMillis
					} catch {
						case _: java.text.ParseException =>
							EnrichmentFilePathFilter.LOG.warn("Invalid path format: {}. Still read.", filePath.getName)
							false
						case _: java.io.IOException =>
							EnrichmentFilePathFilter.LOG.warn("Error listing file: {}. Ignored.", filePath.getPath)
							true
						case e: Throwable =>
							EnrichmentFilePathFilter.LOG.warn("Unknown exception when listing files in: {}\n  {}", Array(filePath.getPath, e.getStackTrace))
							true
					}
				}
	}
}
