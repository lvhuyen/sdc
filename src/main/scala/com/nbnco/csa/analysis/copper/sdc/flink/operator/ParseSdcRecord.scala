package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data._
import com.nbnco.csa.analysis.copper.sdc.flink.source.SdcTarInputFormat
import com.nbnco.csa.analysis.copper.sdc.utils.InvalidPortFormatException
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
  * Created by Huyen on 2/10/18.
  */
object ParseSdcRecord {
	private val LOG = LoggerFactory.getLogger(classOf[ParseSdcRecord[_]])
	var HISTORICAL_BAD_RECORDS_COUNT: Counter = _
	var HISTORICAL_BAD_FILES_COUNT: Counter = _
	var INSTANT_BAD_RECORDS_COUNT: Counter = _
	var INSTANT_BAD_FILES_COUNT: Counter = _

	private val INSTANT_REF_HEADER_COLUMNS = "ifAdminStatus,ifOperStatus,xdslFarEndChannelActualNetDataRateDownstream,xdslChannelActualNetDataRateUpstream,xdslFarEndChannelAttainableNetDataRateDownstream,xdslChannelAttainableNetDataRateUpstream,xdslFarEndLineLoopAttenuationDownstream,extendUserPortFdbUserAddress"
			.split(",")
	private val HISTORICAL_REF_HEADER_COLUMNS = "xdslLinePreviousIntervalSESCounter,xdslLinePreviousIntervalUASCounter,xdslFarEndLinePreviousIntervalLPRCounter,xdslFarEndLinePreviousIntervalSESCounter,xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS,xdslChannelPreviousIntervalUnCorrDtuCounterUS,xdslLinePreviousIntervalReInitCounter,xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS,xdslChannelPreviousIntervalRetransmDtuCounterDS"
			.split(",")
}

class ParseSdcRecord[OutType <: SdcRawBase : SdcParser] extends RichFlatMapFunction[DslamRaw[String], OutType] {
	/***
		* This function finds the pattern of ports.
		* @param rawPort
		* @return: the regex to parse the port
		*/
	private def findPattern(rawPort: String): Option[(Regex, (String, String, String, String) => String)] = {
		for(i <- PORT_PATTERNS.indices) {
			val regex = PORT_PATTERNS(i)._1
			rawPort match {
				case regex(_, _, _, _) =>
					return Some(PORT_PATTERNS(i))
				case _ =>
			}
		}
		None
	}

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		if (ParseSdcRecord.INSTANT_BAD_FILES_COUNT == null) {
			ParseSdcRecord.INSTANT_BAD_FILES_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Instant-Bad-Files-Count")
		}
		if (ParseSdcRecord.INSTANT_BAD_RECORDS_COUNT == null) {
			ParseSdcRecord.INSTANT_BAD_RECORDS_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Instant-Bad-Records-Count")
		}
		if (ParseSdcRecord.HISTORICAL_BAD_FILES_COUNT == null) {
			ParseSdcRecord.HISTORICAL_BAD_FILES_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Historical-Bad-Files-Count")
		}
		if (ParseSdcRecord.HISTORICAL_BAD_RECORDS_COUNT == null) {
			ParseSdcRecord.HISTORICAL_BAD_RECORDS_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Historical-Bad-Records-Count")
		}
	}

	override def flatMap(in: DslamRaw[String], collector: Collector[OutType]): Unit = {
		val actual_cols = in.metadata.columns.split(",")
		val indices = Array.tabulate(actual_cols.length) { i => (actual_cols(i), i) }.toMap

		val (ref, records_cnt, files_cnt) =
			if (in.metadata.isInstant) {
				(ParseSdcRecord.INSTANT_REF_HEADER_COLUMNS.map(indices(_)),
						ParseSdcRecord.INSTANT_BAD_RECORDS_COUNT,
						ParseSdcRecord.INSTANT_BAD_FILES_COUNT)
			}
			else {
				(ParseSdcRecord.HISTORICAL_REF_HEADER_COLUMNS.map(indices(_)),
						ParseSdcRecord.HISTORICAL_BAD_RECORDS_COUNT,
						ParseSdcRecord.HISTORICAL_BAD_FILES_COUNT)
			}


		findPattern(in.data.head._1) match {
			case Some((regex, func)) =>
				in.data.foreach(pair =>
					try {
						val port = pair._1 match {
							case regex(r, s, lt, p) => func(r, s, lt, p)
							case _ => throw new InvalidPortFormatException
						}
						val p = implicitly[SdcParser[OutType]]
						val rec = p.parse(in.metadata.ts, in.metadata.name, port, pair._2, ref)
						collector.collect(rec)
					} catch {
						case e: Throwable =>
							records_cnt.inc()
							ParseSdcRecord.LOG.warn(s"Bad record in file ${in.metadata.relativePath}: ${pair} - ${e.getMessage}")
							ParseSdcRecord.LOG.info("Stacktrace: {}", e.getStackTrace)
					}
				)

			case _ =>
				files_cnt.inc()
				ParseSdcRecord.LOG.warn(s"Bad port format in file ${in.metadata.relativePath}: ${in.data.head._1}")
		}
	}
}
