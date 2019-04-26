//package com.nbnco.csa.analysis.copper.sdc.data
//
//import java.lang
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.{GenericRecord, IndexedRecord}
//
///**
//  * Created by Huyen on 11/7/18.
//  */
//
///**
//  *
//  * @param ts
//  * @param dslam
//  * @param port
//  */
//case class SdcEnrichedInstant(ts: Long,
//                              dslam: String,
//                              port: String,
//                              enrich: SdcDataEnrichment,
//                              data: SdcDataInstant,
//							  correctedAttndr: (Int, Int)
//                             ) extends IndexedRecord with SdcEnrichedBase {
//	def this () = this (0L, "", "", SdcDataEnrichment(), SdcDataInstant(), (-1, -1))
//
//	override def toMap: Map[String, Any] = {
//		Map (
//			"dslam" -> dslam,
//			"port" -> port,
//			"metrics_timestamp" -> ts,
//			"correctedAttainableNetDataRateDownstream" -> correctedAttndr._1,
//			"correctedAttainableNetDataRateUpstream" -> correctedAttndr._2
//		) ++ data.toMap ++ enrich.toMap
//	}
//
//	override def getSchema: Schema = {
//		SdcEnrichedInstant.getSchema()
//	}
//
//	override def get(i: Int): AnyRef = {
//		i match {
//			case 0 => ts.asInstanceOf[AnyRef]
//			case 1 => dslam
//			case 2 => port
//			case 3 => enrich.tsEnrich.asInstanceOf[AnyRef]
//			case 4 => enrich.avcId
//			case 5 => enrich.cpi
//			case 6 => data.ifAdminStatus
//			case 7 => data.ifOperStatus
//			case 8 => data.actualDs
//			case 9 => data.actualUs
//			case 10 => data.attndrDs
//			case 11 => data.attndrUs
//			case 12 => if (data.attenuationDs == null) null else (data.attenuationDs / 10.0f).asInstanceOf[AnyRef]
//			case 13 => data.userMacAddress
//			case 14 => correctedAttndr._1.asInstanceOf[AnyRef]
//			case 15 => correctedAttndr._2.asInstanceOf[AnyRef]
//		}
//	}
//
//	override def put(i: Int, o: scala.Any): Unit = {
//		throw new Exception("This class is for output only")
////		SdcEnrichedInstant.apply()
//	}
//}
//
//object SdcEnrichedInstant {
//	def apply(raw: SdcRawInstant, enrich: EnrichmentData, correctedAttndrDS: Int, correctedAttndrUS: Int): SdcEnrichedInstant = {
//		SdcEnrichedInstant(raw.ts, raw.dslam, raw.port,
//			SdcDataEnrichment(System.currentTimeMillis(),
//				enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
//				enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String],
//				0, 0),
//			raw.data, (correctedAttndrDS, correctedAttndrUS))
//	}
//	def apply(raw: SdcRawInstant, enrich: EnrichmentData): SdcEnrichedInstant = {
//		SdcEnrichedInstant(raw.ts, raw.dslam, raw.port,
//			SdcDataEnrichment(System.currentTimeMillis(),
//				enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
//				enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String],
//				0, 0),
//			raw.data, (-1, -1))
//	}
//	def apply(raw: SdcRawInstant): SdcEnrichedInstant = {
//		SdcEnrichedInstant(raw.ts, raw.dslam, raw.port,
//			SdcDataEnrichment(),
//			raw.data, (-1, -1))
//	}
//	def apply(): SdcEnrichedInstant = {
//		SdcEnrichedInstant(0L, "", "", SdcDataEnrichment(), SdcDataInstant(), (-1, -1))
//	}
//
//	def getSchema(): Schema = {
//		org.apache.avro.SchemaBuilder
//				.record("SdcEnrichedInstant").namespace("com.nbnco")
//				.fields()
//				.name("ts").`type`("long").noDefault()
//				.name("dslam").`type`("string").noDefault()
//				.name("port").`type`("string").noDefault()
//				.name("tsEnrich").`type`("long").noDefault()
//				.name("avcId").`type`().nullable().stringType().noDefault()
//				.name("cpi").`type`().nullable().stringType().noDefault()
//				.name("if_admin_status").`type`("string").noDefault()
//				.name("if_oper_status").`type`("string").noDefault()
//				.name("actual_ds").`type`("int").noDefault()
//				.name("actual_us").`type`("int").noDefault()
//				.name("attndr_ds").`type`("int").noDefault()
//				.name("attndr_us").`type`("int").noDefault()
//				.name("attenuation_ds").`type`().nullable().floatType().noDefault()
//				.name("user_mac_address").`type`().nullable().stringType().noDefault()
//				.name("correctedAttndr_ds").`type`().nullable().intType().noDefault()
//				.name("correctedAttndr_us").`type`().nullable().intType().noDefault()
//				.endRecord()
//	}
//}
