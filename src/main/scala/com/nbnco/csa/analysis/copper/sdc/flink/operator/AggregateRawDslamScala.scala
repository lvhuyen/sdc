//package com.nbnco.csa.analysis.copper.sdc.flink.operator
//
//import com.nbnco.csa.analysis.copper.sdc.data.{DslamMetadata, DslamRaw}
//import com.nbnco.csa.analysis.copper.sdc.flink.source.{DslamCompact, DslamCompactWithType, MergeCsv}
//import java.lang.{Boolean => JBoolean, Long => JLong}
//
//import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
//import org.apache.flink.api.java.tuple.{Tuple4 => JTuple4}
//import org.apache.flink.table.functions.AggregateFunction
//import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
//
//
//class AggregateRawDslamScala extends AggregateFunction[DslamCompactWithType, DslamCompactWithType] {
//
//	override def createAccumulator(): DslamCompactWithType = {
//		new DslamCompactWithType(0: Byte, 0L, "", "", Map.empty)
//	}
//
//	override def getValue(acc: DslamCompactWithType): DslamCompactWithType = acc
//
//	// isInstant, ts, name, columns, data
//	def accumulate(acc: DslamCompactWithType, in: DslamCompactWithType) = {
//		if (acc.f0 == 3 || acc.f0 == in.f0) {
//			//Raise exception here
//			throw InvalidDataException(s"Duplicated data in $DslamMetadata, ${in.f0} at ${in.f1}")
//		} else {
//			val (c, r) = MergeCsv(acc.f3, acc.f4, in.f3, in.f4)
//			acc.f0 += in.f0
//			acc.f1 = in.f1
//			acc.f2 = in.f2
//			acc.f3 = c
//			acc.f4 = r
//		}
//	}
//}
