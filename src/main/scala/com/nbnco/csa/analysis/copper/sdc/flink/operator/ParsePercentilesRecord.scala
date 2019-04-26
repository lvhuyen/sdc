package com.nbnco.csa.analysis.copper.sdc.flink.operator

import scala.util.parsing.json.JSON
import com.nbnco.csa.analysis.copper.sdc.data.JFloat
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class ParsePercentilesRecord extends FlatMapFunction[String, (String, List[JFloat])] {
	override def flatMap(t: String, collector: Collector[(String, List[JFloat])]): Unit = {
		JSON.parseFull(t) match {
			case Some(r: Map[String, Any] @unchecked) =>
				r.get("Percentiles") match {
					case Some(pctls: List[Double] @unchecked) =>
						collector.collect((s"${r.getOrElse("Tier","")},${r.getOrElse("Attenuation",0).asInstanceOf[Double].toInt}",
								pctls.map(r => float2Float(r.toFloat))))
					case _ =>
				}
			case _ =>
				ParsePercentilesRecord.LOG.warn("Bad record in percentiles table: {}", t)
		}
	}
}

object ParsePercentilesRecord {
	private val LOG = LoggerFactory.getLogger(classOf[ParsePercentilesRecord])
}
