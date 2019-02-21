package com.nbnco.csa.analysis.copper.sdc.data

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Created by Huyen on 11/8/18.
  */

/**
  *
  * @param ts           The event time
  * @param dslam        Name of the DSLAM
  * @param port         Port on the DSLAM
  */
case class SdcEnrichedHistorical(ts: Long,
                                 dslam: String,
                                 port: String,
                                 enrich: SdcDataEnrichment,
                                 data: SdcDataHistorical
                                ) extends IndexedRecord with SdcEnrichedBase {
    def this () = this (0L, "", "", SdcDataEnrichment(), SdcDataHistorical())

    override def toMap: Map[String, Any] = {
        Map (
            "dslam" -> dslam,
            "port" -> port,
            "metrics_timestamp" -> ts
        ) ++ data.toMap ++ enrich.toMap
    }

    override def getSchema: Schema = {
        SdcEnrichedHistorical.getSchema()
    }

    override def get(i: Int): AnyRef = {
        i match {
            case 0 => ts.asInstanceOf[AnyRef]
            case 1 => dslam
            case 2 => port
            case 3 => enrich.tsEnrich.asInstanceOf[AnyRef]
            case 4 => enrich.avcId
            case 5 => enrich.cpi
            case 6 => data.ses.asInstanceOf[AnyRef]
            case 7 => data.uas.asInstanceOf[AnyRef]
            case 8 => data.lprFe.asInstanceOf[AnyRef]
            case 9 => data.sesFe.asInstanceOf[AnyRef]
            case 10 => data.unCorrDtuDs.asInstanceOf[AnyRef]
            case 11 => data.unCorrDtuUs.asInstanceOf[AnyRef]
            case 12 => data.reInit.asInstanceOf[AnyRef]
            case 13 => data.reTransUs.asInstanceOf[AnyRef]
            case 14 => data.reTransDs.asInstanceOf[AnyRef]
        }
    }

    override def put(i: Int, o: scala.Any): Unit = {
        throw new Exception("This class is for output only")
//        SdcEnrichedHistorical.apply()
    }
}

object SdcEnrichedHistorical {
    def apply(raw: SdcRawHistorical, enrich: EnrichmentData): SdcEnrichedHistorical = {
        SdcEnrichedHistorical(raw.ts,
            raw.dslam,
            raw.port,
            SdcDataEnrichment(System.currentTimeMillis(),
                enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
                enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String]),
            raw.data)
    }
    def apply(): SdcEnrichedHistorical = SdcEnrichedHistorical(0L, "", "", SdcDataEnrichment(), SdcDataHistorical())

    def getSchema(): Schema = {
        org.apache.avro.SchemaBuilder
                .record("SdcEnrichedHistorical").namespace("com.nbnco")
                .fields()
                .name("ts").`type`("long").noDefault()
                .name("dslam").`type`("string").noDefault()
                .name("port").`type`("string").noDefault()
                .name("tsEnrich").`type`("long").noDefault()
                .name("avcId").`type`().nullable().stringType().noDefault()
                .name("cpi").`type`().nullable().stringType().noDefault()
                .name("ses").`type`("long").noDefault()
                .name("uas").`type`("long").noDefault()
                .name("lprFe").`type`("long").noDefault()
                .name("sesFe").`type`("long").noDefault()
                .name("unCorrDtuDs").`type`("long").noDefault()
                .name("unCorrDtuUs").`type`("long").noDefault()
                .name("reInit").`type`("long").noDefault()
                .name("reTransUs").`type`("long").noDefault()
                .name("reTransDs").`type`("long").noDefault()
                .endRecord()
    }
}
