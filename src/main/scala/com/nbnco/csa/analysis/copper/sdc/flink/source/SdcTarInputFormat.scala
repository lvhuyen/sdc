package com.nbnco.csa.analysis.copper.sdc.flink.source

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.codahale.metrics.SlidingTimeWindowReservoir
import com.nbnco.csa.analysis.copper.sdc.data.{DslamMetadata, DslamRaw, PORT_PATTERN_UNKNOWN}
import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.flink.api.common.io.{CheckpointableInputFormat, FileInputFormat}
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Counter, Histogram, Meter}
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit
import org.apache.flink.util.Preconditions
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Huyen on 8/8/18.
  */

object SdcTarInputFormat {
	val LOG: Logger = LoggerFactory.getLogger(classOf[SdcTarInputFormat])
	val FILE_MOD_TIME_UNKNOWN = Long.MinValue
}

class SdcTarInputFormat(filePath: Path, isComposit: Boolean, metricsPrefix: String) extends FileInputFormat[DslamRaw[String]] (filePath)  with CheckpointableInputFormat[FileInputSplit, Integer]{

	lazy val FILES_COUNT: Counter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC").counter(s"$metricsPrefix-Files-Count")
	lazy val BAD_FILES_COUNT: Counter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC").counter(s"$metricsPrefix-Bad-Files-Count")
	lazy val BAD_RECORDS_COUNT: Counter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC").counter(s"$metricsPrefix-Bad-Records-Count")
	lazy val DELAY_HISTOGRAM: Histogram = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC")
			.histogram(s"$metricsPrefix-Files-Delay", new DropwizardHistogramWrapper(
				new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(15, TimeUnit.MINUTES))))
	lazy val DELAY_METER: Meter = getRuntimeContext.getMetricGroup.addGroup("Chronos-SDC")
			.meter(s"$metricsPrefix-Files-Delay_Meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

	this.unsplittable = true
	this.numSplits = 1

	// --------------------------------------------------------------------------------------------
	//  Variables for internal parsing.
	//  They are all transient, because we do not want them so be serialized
	// --------------------------------------------------------------------------------------------
	@transient
	protected var rawRecords: Iterator[(String, String)] = null

	private var end = false
	protected var offset = 0
	protected var isReOpen = false
	protected var fileName: Path = _
	protected var fileModTime: Long = _

	private var knownHeaders: Set[String] = Set()
	private var portPatternId = PORT_PATTERN_UNKNOWN

	private val METRICS_DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
	METRICS_DATE_FORMAT.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
	private val FILENAME_DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss")
	FILENAME_DATE_FORMAT.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

	// ------------------------ Abstract member -------------------------------
	/**
	  * This function prepares the input data, like parsing headers, or merging data from multiple files
	  * The function returns a tuple of two String if the header is valid, and returns null if invalid
	  * *
	  *
	  * @return Returns the read record if it was successfully deserialized.
	  */
	@throws[IOException]
	private def readMemberTextFile(tarStream: TarArchiveInputStream): (Map[String, String], Map[String, String]) = {
		tarStream.getNextTarEntry
		val lines = scala.io.Source.fromInputStream(tarStream).getLines
		val (first_half, second_half) = lines.span(!_.startsWith("Object ID"))

		val tmp_h = first_half.filter(!_.isEmpty)
				.map(_.span(_ != ',')) ++ Iterator(("Columns", second_half.next.dropWhile(_ != ',')))
		val headers = tmp_h.toMap[String, String].mapValues(_.drop(1))

		val tmp_r = second_half.map(_.span(_ != ','))
		val records = (
				if (headers("Object Type").equalsIgnoreCase("Current MAC Address"))
					tmp_r.map(kvp => (kvp._1.split(".ITF")(0), kvp._2))
				else tmp_r
				).toMap[String, String]
				.mapValues(_.drop(1))
		(headers, records)
	}

	@throws[IOException]
	private def readCompositTarFile(tarStream: TarArchiveInputStream): (Map[String, String], Map[String, String]) = {
		val (headers_1, records_1) = readMemberTextFile(tarStream)
		val (headers_2, records_2) = readMemberTextFile(tarStream)

		if (!(headers_1("Time stamp").equals(headers_2("Time stamp")) && headers_1("NE Name").equals(headers_2("NE Name"))))
			throw InvalidDataException(s"Tar file with unmatching member files: ${headers_1("Time stamp")}, ${headers_1("NE Name")}")

		def mergeData (h1: Map[String, String], r1: Map[String, String], h2: Map[String, String], r2: Map[String, String]): (Map[String, String], Map[String, String]) = {
			r2.keySet.diff(r1.keySet).foreach({
				p => SdcTarInputFormat.LOG.warn(s"Port $p of ${h1("NE Name")} has MAC but no other data: ")
			})
			(h1 + ("Columns" -> s"${h1("Columns")},${h2("Columns")}"),
					r1.map(a => a._1 -> (a._2 + "," + r2.getOrElse(a._1, ""))))
		}

		if (headers_1("Object Type").equals("XDSL Port"))
			mergeData(headers_1, records_1, headers_2, records_2)
		else
			mergeData(headers_2, records_2, headers_1, records_1)
	}

	@throws[IOException]
	private def prepareData(): DslamRaw[String] = {
		val tarStream = new TarArchiveInputStream(this.stream)
		try {
			this.portPatternId = PORT_PATTERN_UNKNOWN
			val processingTime = new java.util.Date().getTime

			val (headers, records) =
				if (isComposit) readCompositTarFile(tarStream)
			else
				readMemberTextFile(tarStream)

			// Track the list of known columns headers order:
			if (!knownHeaders.contains(headers("Columns"))) {
				SdcTarInputFormat.LOG.info(s"New columns header found in ${this.fileName.getPath}: ${headers("Columns")}")
				knownHeaders += headers("Columns")
			}

			// cache the data which helps parsing records faster
			val metricsTime = this.METRICS_DATE_FORMAT.parse(headers("Time stamp")).getTime

			// Validate data timestamp and filename timestamp
			// todo: enable this check if needed
//			val fileNameTs = this.FILENAME_DATE_FORMAT.parse(this.fileName.getName.substring(0, 15))
//			if (scala.math.abs(fileNameTs.getTime - metricsTime) > SdcTarInputFormat.MAX_TIME_DIFF) {
//				throw InvalidDataException(s"Abnormal timestamps: in data $metricsTime vs. in filename $fileNameTs")
//			}

			// Update metrics for time arrival delay
			if (this.fileModTime != SdcTarInputFormat.FILE_MOD_TIME_UNKNOWN) {
				this.DELAY_HISTOGRAM.update((this.fileModTime - metricsTime) / 1000)
				this.DELAY_METER.markEvent((this.fileModTime - metricsTime) / 1000)
			}

			if (records.nonEmpty)
				DslamRaw(
					DslamMetadata(isComposit, headers("NE Name"), metricsTime, headers("Columns"), fileName.getPath.split(filePath.getPath)(1), fileModTime, processingTime),
					records.toSeq)
			else
				null
		} finally {
			tarStream.close()
		}
	}

	private var charsetName = "UTF-8"
	def getCharsetName: String = charsetName
	def setCharsetName(charsetName: String): Unit = {
		if (charsetName == null) throw new IllegalArgumentException("Charset must not be null.")
		this.charsetName = charsetName
	}

	@throws[IOException]
	override def nextRecord(record: DslamRaw[String]): DslamRaw[String] = {
		this.end = true
		try {
			prepareData()
		} catch {
			case e: Throwable =>
				this.BAD_FILES_COUNT.inc()
				SdcTarInputFormat.LOG.warn(s"Error reading file: ${this.fileName.getPath}. Reason: ${e.getMessage}")
				SdcTarInputFormat.LOG.info(s"Stacktrace: {}", e.getStackTrace)
				if (SdcTarInputFormat.LOG.isDebugEnabled) throw e
				null
		}
	}

	override def reachedEnd: Boolean = this.end

	/**
	  * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	  * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	  *
	  * @param split The input split to open.
	  * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	  */
	@throws[IOException]
	override def open(split: FileInputSplit): Unit = {
		this.fileName = split.getPath
//
//		if (this.FILES_COUNT == null) {
//			this.FILES_COUNT = getRuntimeContext
//					.getMetricGroup.addGroup("Chronos-SDC")
//					.counter(s"$metricsPrefix-Files-Count")
//		}
//		if (this.BAD_FILES_COUNT == null) {
//			this.BAD_FILES_COUNT = getRuntimeContext
//					.getMetricGroup.addGroup("Chronos-SDC")
//					.counter(s"$metricsPrefix-Bad-Files-Count")
//		}
//		if (this.BAD_RECORDS_COUNT == null) {
//			this.BAD_RECORDS_COUNT = getRuntimeContext
//					.getMetricGroup.addGroup("Chronos-SDC")
//					.counter(s"$metricsPrefix-Bad-Records-Count")
//		}
//		if (this.DELAY_HISTOGRAM == null) {
//			this.DELAY_HISTOGRAM = getRuntimeContext
//					.getMetricGroup.addGroup("Chronos-SDC")
//					.histogram(s"$metricsPrefix-Files-Delay", new DropwizardHistogramWrapper(
//						new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(120, TimeUnit.MINUTES))))
//		}
//		if (this.DELAY_METER == null) {
//			this.DELAY_METER = getRuntimeContext
//					.getMetricGroup.addGroup("Chronos-SDC")
//					.meter(s"$metricsPrefix-Files-Delay_Meter", new DropwizardMeterWrapper(
//						new com.codahale.metrics.Meter()))
//		}

		if (this.splitStart != 0) {
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			throw new IOException("File should NOT be splittable")
		}
		else {
			this.FILES_COUNT.inc()
			this.end = false

			this.fileModTime = split match {
				case s: TimestampedFileInputSplit => s.getModificationTime
				case _ => SdcTarInputFormat.FILE_MOD_TIME_UNKNOWN
			}

			try {
				super.open(split)
			} catch {
				case e: Throwable =>
					this.end = true
					this.BAD_FILES_COUNT.inc()
					SdcTarInputFormat.LOG.warn(s"Error reading file: ${this.fileName.getPath}. Reason: ${e.getMessage}")
					SdcTarInputFormat.LOG.info(s"Stacktrace: {}", e.getStackTrace)
					if (SdcTarInputFormat.LOG.isDebugEnabled) throw e
			}
		}
	}

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
