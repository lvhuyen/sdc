package com.nbnco.csa.analysis.copper.sdc.flink.operator

import scala.util.parsing.json.JSON
import com.nbnco.csa.analysis.copper.sdc.data.{JavaFloat}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ParsePercentilesRecord extends FlatMapFunction[String, (String, List[JavaFloat])] {
	override def flatMap(t: String, collector: Collector[(String, List[JavaFloat])]): Unit = {
		JSON.parseFull(t) match {
			case Some(r: Map[String, Any]) =>
				r.get("Percentiles") match {
					case Some(pctls: List[Double]) =>
						collector.collect((s"${r.getOrElse("Tier","")},${r.getOrElse("Attenuation",0).asInstanceOf[Double].toInt}",
								pctls.map(r => float2Float(r.toFloat))))
					case _ =>
				}
			case _ =>
		}
	}
}
