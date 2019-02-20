package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data.EnrichmentRecord
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
			collector.collect(EnrichmentRecord(v(4).toLong, v(0),v(1),v(2),v(3)))
		} catch {
			case _: Throwable =>
				ParseFlsRecord.LOG.warn("In valid fls record: {}", t)
		}
	}
}
