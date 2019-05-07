package com.starfox.analysis.copper.sdc.flink.source

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.codahale.metrics.SlidingTimeWindowReservoir
import com.starfox.analysis.copper.sdc.data.DslamType._
import com.starfox.analysis.copper.sdc.data.{DslamMetadata, DslamRaw, PortFormatConverter, mergeCsv}
import com.starfox.analysis.copper.sdc.utils.InvalidDataException
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.flink.api.common.io.{CheckpointableInputFormat, FileInputFormat}
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Counter, Histogram, Meter}
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit
import org.apache.flink.util.Preconditions
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.util.{Failure, Success}

/**
  * Created by Huyen on 8/8/18.
  */

object SdcTarInputFormat {
	val LOG: Logger = LoggerFactory.getLogger(classOf[SdcTarInputFormat])
	val FILE_MOD_TIME_UNKNOWN = Long.MinValue
}

class SdcTarInputFormat(filePath: Path, metricsPrefix: String, truncatePathWhileLoggingUpToLevel: Int = 0)
		extends FileInputFormat[DslamRaw[Map[String,String]]] (filePath)
				with CheckpointableInputFormat[FileInputSplit, Integer] {

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
//	private var portPatternId = PORT_PATTERN_UNKNOWN

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
	  * @return Returns one FileOutput - a tuple4, with:
		*         first element is the last Modified Date of the inner file
		*         second element is the headers (a map from header name to header value)
	  	*         third element is the records (a map from port to data)
	  	*         fourth element is the number of file read (always 1 in this case)
	  */
	type FileOutput = (Long, Map[String, String], Map[String, String], Int)
	@throws[IOException]
	private def readTarEntryFromStream(tarStream: TarArchiveInputStream): FileOutput = {
		val lines = scala.io.Source.fromInputStream(tarStream).getLines
		val (first_half, second_half) = lines.span(!_.startsWith("Object ID"))

		val tmp_h = first_half.filter(!_.isEmpty)
				.map(_.span(_ != ',')) ++ Iterator(("Columns", second_half.next.dropWhile(_ != ',')))
		val headers = tmp_h.toMap[String, String].mapValues(_.drop(1))

		val tmp_r = second_half.map(_.span(_ != ',')).toMap[String, String]
		PortFormatConverter.convertPortAndManipulateValue[String](tmp_r, _.drop(1)) match {
			case Success(records) =>
				(tarStream.getCurrentEntry.getLastModifiedDate.getTime, headers, records, 1)
			case Failure(exception) =>
				throw exception
		}
	}

	@throws[IOException]
	@tailrec
	private def readCompositTarFile(tarStream: TarArchiveInputStream, accumulator: FileOutput): FileOutput = {
		if (tarStream.getNextTarEntry == null)
			accumulator
		else {
			val curOutput = readTarEntryFromStream(tarStream)
			val newAccumulator =
				if (accumulator._4 == 0)
					curOutput
				else {
					if (!(accumulator._2("Time stamp").equals(curOutput._2("Time stamp")) &&
							accumulator._2("NE Name").equals(curOutput._2("NE Name"))))
						throw InvalidDataException(s"Tar file with unmatched member files: ${curOutput._2("Time stamp")}, ${curOutput._3("NE Name")}")
					(
							Math.max(accumulator._1, curOutput._1),
							accumulator._2 + ("Columns" -> s"${accumulator._2("Columns")},${curOutput._2("Columns")}"),
							mergeCsv(accumulator._2("Columns"), accumulator._3, curOutput._2("Columns"), curOutput._3),
							accumulator._4 + 1
					)
				}
			readCompositTarFile(tarStream, newAccumulator)
		}
	}

	@throws[IOException]
	private def prepareData(): DslamRaw[Map[String, String]] = {
		val tarStream = new TarArchiveInputStream(this.stream)
		try {
//			this.portPatternId = PORT_PATTERN_UNKNOWN
			val processingTime = new java.util.Date().getTime

			val (tarComponentFileTime, headers, records, fileCount) =
					readCompositTarFile(tarStream, (0, Map.empty, Map.empty, 0))

			// Track the list of known columns headers order:
			if (!knownHeaders.contains(headers("Columns"))) {
				SdcTarInputFormat.LOG.info(s"New columns header found in ${this.fileName.getPath}: ${headers("Columns")}")
				knownHeaders += headers("Columns")
			}

			// cache the data which helps parsing records faster
			val metricsTime = this.METRICS_DATE_FORMAT.parse(headers("Time stamp")).getTime

			// Update metrics for time arrival delay
			if (this.fileModTime != SdcTarInputFormat.FILE_MOD_TIME_UNKNOWN) {
				this.DELAY_HISTOGRAM.update((this.fileModTime - metricsTime) / 1000)
				this.DELAY_METER.markEvent((this.fileModTime - metricsTime) / 1000)
			}

//			DslamRaw(metricsTime, headers("NE Name"), if (fileCount == 1) DSLAM_HISTORICAL else DSLAM_INSTANT, records,
//				DslamMetadata(headers("Columns"), fileName.getPath.split(filePath.getPath)(1), fileModTime, processingTime, tarComponentFileTime, records.size))
			DslamRaw(metricsTime, headers("NE Name"), if (fileCount == 1) DSLAM_HISTORICAL else DSLAM_INSTANT, records,
				DslamMetadata(headers("Columns"), fileName.getPath.split("/", truncatePathWhileLoggingUpToLevel + 2).last, fileModTime, processingTime, tarComponentFileTime, records.size))
		} finally {
			tarStream.close()
		}
	}


	override def nextRecord(record: DslamRaw[Map[String,String]]): DslamRaw[Map[String,String]] = {
		try {
			prepareData()
		} catch {
			case e: Exception =>
				this.BAD_FILES_COUNT.inc()
				SdcTarInputFormat.LOG.warn(s"Error reading file: ${this.fileName.getPath}. Reason: ${e.getLocalizedMessage}")
				SdcTarInputFormat.LOG.info(s"Stacktrace: {}", e.getStackTrace)
				if (SdcTarInputFormat.LOG.isDebugEnabled) throw e
				null
		} finally {
			this.end = true
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
