package com.nbnco.csa.analysis.copper.sdc.flink

import java.lang.{Long => JLong, Integer => JInteger, Boolean => JBoolean, Byte => JByte}
import org.apache.flink.api.java.tuple.{Tuple4 => JTuple4, Tuple5 => JTuple5}

package object source {
	def MergeCsv(c1: String, r1: Map[String, String], c2: String, r2: Map[String, String]): (String, Map[String, String]) = {
		val d1blank = c1.filter(_.equals(','))
		val d2blank = c2.filter(_.equals(','))

		val d = (r1.keySet ++ r2.keySet).map(k => (k, s"${r1.getOrElse(k, d1blank)},${r2.getOrElse(k, d2blank)}")).toMap
		(s"$c1,$c2", d)
	}

//	type DslamCompact = JTuple4[JLong, String, String, Map[String, String]]
	type DslamCompactWithType = JTuple5[JByte, JLong, String, String, Map[String, String]]
}
