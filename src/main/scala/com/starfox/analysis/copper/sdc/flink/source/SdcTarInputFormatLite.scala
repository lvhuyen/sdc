package com.starfox.analysis.copper.sdc.flink.source

import java.io.IOException

import com.starfox.analysis.copper.sdc.data.{DslamMetadata, DslamRaw, DslamType}
import com.starfox.analysis.copper.sdc.flink.source.SdcTarInputFormatLite._
import org.apache.flink.api.common.io.{CheckpointableInputFormat, FileInputFormat}
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Preconditions
import org.slf4j.{Logger, LoggerFactory}

object SdcTarInputFormatLite {
	val LOG: Logger = LoggerFactory.getLogger(classOf[SdcTarInputFormat])
}

class SdcTarInputFormatLite(filePath: Path, metricsPrefix: String)
		extends FileInputFormat[DslamRaw[FileInputSplit]] (filePath)
				with CheckpointableInputFormat[FileInputSplit, Integer] {

	this.unsplittable = true
	this.numSplits = 1

	lazy val FILES_COUNT: Counter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC").counter(s"$metricsPrefix-Files-Count")
	lazy val BAD_FILES_COUNT: Counter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC").counter(s"$metricsPrefix-Bad-Files-Count")

	private var end = false
	private var file: FileInputSplit = _

	private val FILENAME_PATTERN = """^([^\W_]*)_([ihIH])[^_]*_(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}).*""".r
	private val FILENAME_DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm")
	FILENAME_DATE_FORMAT.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

	override def open(split: FileInputSplit): Unit = {
		if (this.splitStart != 0) {
			throw new IOException("File should NOT be splittable")
		}
		else {
			this.FILES_COUNT.inc()
			this.file = split
			this.end = false
		}
	}

	override def nextRecord(ot: DslamRaw[FileInputSplit]): DslamRaw[FileInputSplit] = {
		try {
			this.file.getPath.getName match {
				case FILENAME_PATTERN(dslamName, dslamType, datetime) =>
					DslamRaw(FILENAME_DATE_FORMAT.parse(datetime).getTime,
						dslamName,
						if (dslamType.equalsIgnoreCase("I")) DslamType.DSLAM_INSTANT else DslamType.DSLAM_HISTORICAL,
						this.file, DslamMetadata.EMPTY
					)
			}
		} catch {
			case e: IOException =>
				this.BAD_FILES_COUNT.inc()
				LOG.warn(s"Filename in invalid format: ${this.file.getPath.getPath}. Reason: ${e.getMessage}")
				null
		} finally {
			this.end = true
		}
	}

	override def reachedEnd: Boolean = this.end

	@throws[IOException]
	override def getCurrentState = 0

	@throws[IOException]
	override def reopen(split: FileInputSplit, state: Integer): Unit = {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.")
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.")
		Preconditions.checkArgument((state == -1) || state >= split.getStart, " Illegal offset " + state + ", smaller than the splits start=" + split.getStart, "")

		this.open(split)
	}
}
