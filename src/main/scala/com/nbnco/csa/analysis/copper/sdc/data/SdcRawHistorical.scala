//package com.nbnco.csa.analysis.copper.sdc.data
//
///**
//  * Created by Huyen on 11/8/18.
//  */
//
///**
//  *
//  * @param ts    The event time
//  * @param dslam        Name of the DSLAM
//  * @param port         Port on the DSLAM
//  */
//case class SdcRawHistorical(ts: Long, dslam: String, port: String,
//                            data: SdcDataHistorical
//                      ) extends SdcRawBase {
//
//	def enrich(enrich: EnrichmentData): SdcEnrichedHistorical = {
//		SdcEnrichedHistorical(this, enrich)
//	}
//
//	override def toMap: Map[String, Any] = {
//		Map (
//			"dslam" -> dslam,
//			"port" -> port,
//			"metrics_timestamp" -> ts
//		) ++ data.toMap
//	}
//}
//
//object SdcRawHistorical extends SdcParser[SdcRawHistorical] {
//	override def parse(ts: Long, dslam: String, port: String, raw: String, ref: Array[Int]): SdcRawHistorical = {
//		val v = raw.split(',')
//		new SdcRawHistorical(ts, dslam, port, SdcDataHistorical(v(ref(0)).toShort, v(ref(1)).toShort, v(ref(2)).toShort, v(ref(3)).toShort, v(ref(4)).toShort, v(ref(5)).toShort, v(ref(6)).toShort, v(ref(7)).toShort, v(ref(8)).toShort))
//	}
//}