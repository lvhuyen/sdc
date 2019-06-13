package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data.{EnrichmentAttributeName, EnrichmentRecord}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 5/9/18.
  */

object ParseFlsRecord {
	private val LOG = LoggerFactory.getLogger(classOf[ParseFlsRecord])
	def apply(): ParseFlsRecord = {
		new ParseFlsRecord
	}
}

class ParseFlsRecord extends FlatMapFunction[String, EnrichmentRecord] {
	override def flatMap(t: String, collector: Collector[EnrichmentRecord]): Unit = {
		val v = t.split(",")
		try {
			collector.collect(EnrichmentRecord(v(4).toLong, v(1), v(2),
				Map(EnrichmentAttributeName.AVC -> v(3),EnrichmentAttributeName.CPI -> v(4))))
		} catch {
			case _: Throwable =>
				ParseFlsRecord.LOG.warn("Invalid fls record: {}", t)
		}
	}
}
