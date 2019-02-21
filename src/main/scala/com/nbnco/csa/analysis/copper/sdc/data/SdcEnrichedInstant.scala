package com.nbnco.csa.analysis.copper.sdc.data

import java.lang

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}

/**
  * Created by Huyen on 11/7/18.
  */

/**
  *
  * @param ts
  * @param dslam
  * @param port
  */
case class SdcEnrichedInstant(ts: Long,
                              dslam: String,
                              port: String,
                              enrich: SdcDataEnrichment,
                              data: SdcDataInstant
                             ) extends IndexedRecord with SdcEnrichedBase {
	def this () = this (0L, "", "", SdcDataEnrichment(), SdcDataInstant())

	override def toMap: Map[String, Any] = {
		Map (
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts
		) ++ data.toMap ++ enrich.toMap
	}

	override def getSchema: Schema = {
		SdcEnrichedInstant.getSchema()
	}

	override def get(i: Int): AnyRef = {
		i match {
			case 0 => ts.asInstanceOf[AnyRef]
			case 1 => dslam
			case 2 => port
			case 3 => enrich.tsEnrich.asInstanceOf[AnyRef]
			case 4 => enrich.avcId
			case 5 => enrich.cpi
			case 6 => data.if_admin_status
			case 7 => data.if_oper_status
			case 8 => data.actual_ds.asInstanceOf[AnyRef]
			case 9 => data.actual_us.asInstanceOf[AnyRef]
			case 10 => data.attndr_ds.asInstanceOf[AnyRef]
			case 11 => data.attndr_us.asInstanceOf[AnyRef]
			case 12 => data.attenuation_ds
			case 13 => data.user_mac_address
		}
	}

	override def put(i: Int, o: scala.Any): Unit = {
		throw new Exception("This class is for output only")
//		SdcEnrichedInstant.apply()
	}
}

object SdcEnrichedInstant {
	def apply(raw: SdcRawInstant, enrich: EnrichmentData): SdcEnrichedInstant = {
		SdcEnrichedInstant(raw.ts, raw.dslam, raw.port,
			SdcDataEnrichment(System.currentTimeMillis(),
				enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
				enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String]),
			raw.data)
	}
	def apply(raw: SdcRawInstant): SdcEnrichedInstant = {
		SdcEnrichedInstant(raw.ts, raw.dslam, raw.port,
			SdcDataEnrichment(Long.MinValue, null, null),
			raw.data)
	}
	def apply(): SdcEnrichedInstant = {
		SdcEnrichedInstant(0L, "", "", SdcDataEnrichment(), SdcDataInstant())
	}

	def buildKeyForPctlsTable(enrich: EnrichmentData) = s"${
		enrich.get(EnrichmentAttributeName.TECH_TYPE) match {
			case TechType.FTTN =>
				s"FTTN"
		}

		enrich.getOrElse(EnrichmentAttributeName.TECH_TYPE, "")},${
		if (isDownStream) enrich.getOrElse(EnrichmentAttributeName.TC4_DS, "")
		else enrich.getOrElse(EnrichmentAttributeName.TC4_US, "")},${

	}"

	def getSchema(): Schema = {
		org.apache.avro.SchemaBuilder
				.record("SdcEnrichedInstant").namespace("com.nbnco")
				.fields()
				.name("ts").`type`("long").noDefault()
				.name("dslam").`type`("string").noDefault()
				.name("port").`type`("string").noDefault()
				.name("tsEnrich").`type`("long").noDefault()
				.name("avcId").`type`().nullable().stringType().noDefault()
				.name("cpi").`type`().nullable().stringType().noDefault()
				.name("if_admin_status").`type`("string").noDefault()
				.name("if_oper_status").`type`("string").noDefault()
				.name("actual_ds").`type`("int").noDefault()
				.name("actual_us").`type`("int").noDefault()
				.name("attndr_ds").`type`("int").noDefault()
				.name("attndr_us").`type`("int").noDefault()
				.name("attenuation_ds").`type`().nullable().intType().noDefault()
				.name("user_mac_address").`type`().nullable().stringType().noDefault()
				.endRecord()
	}
}
