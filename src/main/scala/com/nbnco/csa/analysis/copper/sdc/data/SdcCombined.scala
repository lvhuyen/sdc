package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * Created by Huyen on 17/8/18.
  */

/**
  */
case class SdcCombined(ts: Long, dslam: String, port: String,
                       dataI: SdcDataInstant,
                       dataH: SdcDataHistorical,
                       enrich: SdcDataEnrichment
                ) extends IndexedRecord with CopperLine {

    override def toMap: Map[String, Any] = {
        Map (
            "dslam" -> dslam,
            "port" -> port,
            "metrics_timestamp" -> ts
        ) ++ dataI.toMap ++ dataH.toMap ++ enrich.toMap
    }

    override def getSchema: Schema = {
        SdcCombined.SCHEMA
    }

    override def get(i: Int): AnyRef = {
        i match {
            case 0 => ts.asInstanceOf[AnyRef]
            case 1 => dslam
            case 2 => port
            case 3 => dataI.ifAdminStatus
            case 4 => dataI.ifOperStatus
            case 5 => dataI.actualDs
            case 6 => dataI.actualUs
            case 7 => dataI.attndrDs
            case 8 => dataI.attndrUs
            case 9 => if (dataI.attenuationDs == null) null else (dataI.attenuationDs / 10.0f).asInstanceOf[AnyRef]
            case 10 => dataI.userMacAddress
            case 11 => dataH.ses
            case 12 => dataH.uas
            case 13 => dataH.lprFe
            case 14 => dataH.sesFe
			case 15 => dataH.reInit
            case 16 => dataH.unCorrDtuDs
            case 17 => dataH.unCorrDtuUs
            case 18 => dataH.reTransUs
            case 19 => dataH.reTransDs
            case 20 => enrich.tsEnrich.asInstanceOf[AnyRef]
            case 21 => enrich.avcId
            case 22 => enrich.cpi
            case 23 => enrich.corrAttndrDs
            case 24 => enrich.corrAttndrUs
        }
    }

    override def put(i: Int, o: scala.Any): Unit = {
        throw new Exception("This class is for output only")
    }

//    override def enrich(enrich: EnrichmentData): SdcEnrichedBase = {
//        this
//    }
}

object SdcCombined {
	implicit val typeInfo = TypeInformation.of(classOf[SdcCombined])

	val EMPTY = SdcCombined(0, "", "", SdcDataInstant.EMPTY, SdcDataHistorical.EMPTY, SdcDataEnrichment.EMPTY)

	def apply(): SdcCombined = EMPTY

	def apply(ts: Long, dslam: String, port: String, raw: String, ref: Array[Int]): SdcCombined = {
		val v = raw.split(",", -1)

		val i = if (v(ref(0)) != "" || v(ref(1)) != "")
			SdcDataInstant(
				v(ref(0)).equals("up"),
				v(ref(1)).equals("up"),
				v(ref(2)).toInt,
				v(ref(3)).toInt,
				v(ref(4)).toInt,
				v(ref(5)).toInt,
				if (v(ref(6)).equals("")) -1: Short else v(ref(6)).toShort,
				v(ref(7))
			) else SdcDataInstant.EMPTY

		val h = if (v(ref(8)) != "")
			SdcDataHistorical(
				v(ref(8)).toShort,
				v(ref(9)).toShort,
				v(ref(10)).toShort,
				v(ref(11)).toShort,
				v(ref(12)).toShort,
				v(ref(13)).toLong,
				v(ref(14)).toLong,
				v(ref(15)).toLong,
				v(ref(16)).toLong
			) else SdcDataHistorical.EMPTY

		new SdcCombined(ts, dslam, port, i, h, SdcDataEnrichment.EMPTY)
	}

	def apply(raw: SdcCombined, enrich: EnrichmentData, correctedAttndrDS: Int, correctedAttndrUS: Int): SdcCombined =
		raw.copy(enrich = SdcDataEnrichment(System.currentTimeMillis(),
			enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
			enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String],
			correctedAttndrDS, correctedAttndrUS)
		)

	val REF_COLUMNS_NAME: Array[String] = Array(
		"ifAdminStatus",
		"ifOperStatus",
		"xdslFarEndChannelActualNetDataRateDownstream",
		"xdslChannelActualNetDataRateUpstream",
		"xdslFarEndChannelAttainableNetDataRateDownstream",
		"xdslChannelAttainableNetDataRateUpstream",
		"xdslFarEndLineLoopAttenuationDownstream",
		"extendUserPortFdbUserAddress",
		"xdslLinePreviousIntervalSESCounter",
		"xdslLinePreviousIntervalUASCounter",
		"xdslFarEndLinePreviousIntervalLPRCounter",
		"xdslFarEndLinePreviousIntervalSESCounter",
		"xdslLinePreviousIntervalReInitCounter",
		"xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS",
		"xdslChannelPreviousIntervalUnCorrDtuCounterUS",
		"xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS",
		"xdslChannelPreviousIntervalRetransmDtuCounterDS"
	)


	lazy val SCHEMA: Schema = {
		org.apache.avro.SchemaBuilder
				.record("SdcCombined").namespace("com.nbnco")
				.fields()
				.name("ts").`type`("long").noDefault()
				.name("dslam").`type`("string").noDefault()
				.name("port").`type`("string").noDefault()
				.name("ifAdminStatus").`type`("boolean").noDefault()
				.name("ifOperStatus").`type`("boolean").noDefault()
				.name("actualDs").`type`("int").noDefault()
				.name("actualUs").`type`("int").noDefault()
				.name("attndrDs").`type`("int").noDefault()
				.name("attndrUs").`type`("int").noDefault()
				.name("attenuationDs").`type`().nullable().floatType().noDefault()
				.name("userMacAddress").`type`().nullable().stringType().noDefault()
				.name("ses").`type`().nullable().intType().noDefault()
				.name("uas").`type`().nullable().intType().noDefault()
				.name("lprFe").`type`().nullable().intType().noDefault()
				.name("sesFe").`type`().nullable().intType().noDefault()
				.name("reInit").`type`().nullable().intType().noDefault()
				.name("unCorrDtuDs").`type`().nullable().longType().noDefault()
				.name("unCorrDtuUs").`type`().nullable().longType().noDefault()
				.name("reTransUs").`type`().nullable().longType().noDefault()
				.name("reTransDs").`type`().nullable().longType().noDefault()
				.name("tsEnrich").`type`().nullable().longType().noDefault()
				.name("avc").`type`().nullable().stringType().noDefault()
				.name("cpi").`type`().nullable().stringType().noDefault()
				.name("correctedAttndrDs").`type`().nullable().intType().noDefault()
				.name("correctedAttndrUs").`type`().nullable().intType().noDefault()
				.endRecord()


//				.name("ses").`type`("int").noDefault()
//				.name("uas").`type`("int").noDefault()
//				.name("lprFe").`type`("int").noDefault()
//				.name("sesFe").`type`("int").noDefault()
//				.name("reInit").`type`("int").noDefault()
//				.name("unCorrDtuDs").`type`("long").noDefault()
//				.name("unCorrDtuUs").`type`("long").noDefault()
//				.name("reTransUs").`type`("long").noDefault()
//				.name("reTransDs").`type`("long").noDefault()
//				.name("tsEnrich").`type`("long").noDefault()


	}
}
