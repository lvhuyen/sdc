package com.nbnco.csa.analysis.copper.sdc.flink.operator

import ParseCombinedSdcRecord._
import com.nbnco.csa.analysis.copper.sdc.data._
import com.nbnco.csa.analysis.copper.sdc.utils.InvalidPortFormatException
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.DataStream

object ParseCombinedSdcRecord {
	private val LOG = LoggerFactory.getLogger(classOf[ParseCombinedSdcRecord])
	var BAD_RECORDS_COUNT: Counter = _
	var BAD_FILES_COUNT: Counter = _

	def apply(inputStream: DataStream[DslamRaw[Map[String,String]]]): DataStream[SdcCombined] = {
		inputStream.flatMap(new ParseCombinedSdcRecord)
				.uid(OperatorId.SDC_PARSER)
				.name("Parse SDC Records")
	}
}


class ParseCombinedSdcRecord extends RichFlatMapFunction[DslamRaw[Map[String,String]], SdcCombined] {
	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		if (BAD_FILES_COUNT == null) {
			BAD_FILES_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Sdc-Bad-Files-Count")
		}
		if (BAD_RECORDS_COUNT == null) {
			BAD_RECORDS_COUNT = getRuntimeContext
					.getMetricGroup.addGroup("Chronos-SDC")
					.counter("Sdc-Bad-Records-Count")
		}
	}

	override def flatMap(in: DslamRaw[Map[String,String]], collector: Collector[SdcCombined]): Unit = {
		if (in.data.nonEmpty) {
			val actual_cols = in.metadata.columns.split(",")
			val indices = Array.tabulate(actual_cols.length) { i => (actual_cols(i), i) }.toMap
			val ref = SdcCombined.REF_COLUMNS_NAME.map(indices(_))

			in.data.foreach(line =>
				try {
					val tmp = SdcCombined(in.ts, in.name, line._1, line._2, ref)
					collector.collect(tmp)
				} catch {
					case e@(_: java.lang.NumberFormatException | _: InvalidPortFormatException) =>
						BAD_RECORDS_COUNT.inc()
						LOG.warn(s"Bad record in data file for ${in.name}@${in.ts}: ${line} - ${e.getMessage}")

						case exception: Exception => throw exception
				}
			)
		}
	}
}
