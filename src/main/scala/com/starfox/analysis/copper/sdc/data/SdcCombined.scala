package com.starfox.analysis.copper.sdc.data

import java.lang.{Boolean => JBool, Long => JLong, Short => JShort, Float => JFloat}

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

/**
  * Created by Huyen on 17/8/18.
  */

/**
  */
case class SdcCombined(ts: Long, dslam: String, port: String,
                       dataI: SdcDataInstant,
                       dataH: Option[SdcDataHistorical],
                       enrich: Option[SdcDataEnrichment]
                ) extends IndexedRecord with CopperLine {

    override def toMap: Map[String, Any] = {
		Map (
            "dslam" -> dslam,
            "port" -> port,
            "metrics_timestamp" -> ts
        ) ++ dataI.toMap ++ dataH.map(_.toMap).getOrElse(Map.empty) ++ enrich.map(_.toMap).getOrElse(Map.empty)
    }

    override def getSchema: Schema = {
        SdcCombined.SCHEMA
    }

    override def get(i: Int): AnyRef = {
        i match {
            case 0 => ts: JLong
            case 1 => dslam
            case 2 => port
            case 3 => dataI.ifAdminStatus: JBool
            case 4 => dataI.ifOperStatus: JBool
            case 5 => dataI.actualDs: Integer
            case 6 => dataI.actualUs: Integer
            case 7 => dataI.attndrDs: Integer
            case 8 => dataI.attndrUs: Integer
            case 9 => dataI.attenuationDs.map(_/10.0f: JFloat).orNull
            case 10 => dataI.userMacAddress
            case 11 => dataH.map(_.ses: JShort).orNull
            case 12 => dataH.map(_.uas: JShort).orNull
            case 13 => dataH.map(_.lprFe: JShort).orNull
            case 14 => dataH.map(_.sesFe: JShort).orNull
			case 15 => dataH.map(_.reInit: JShort).orNull
            case 16 => dataH.map(_.unCorrDtuDs: JLong).orNull
            case 17 => dataH.map(_.unCorrDtuUs: JLong).orNull
            case 18 => dataH.map(_.reTransUs: JLong).orNull
            case 19 => dataH.map(_.reTransDs: JLong).orNull
            case 20 => enrich.map(_.ts: JLong).orNull
            case 21 => enrich.map(_.avc).orNull
            case 22 => enrich.map(_.cpi).orNull
            case 23 => enrich.map(_.corrAttndrDs: Integer).orNull
            case 24 => enrich.map(_.corrAttndrUs: Integer).orNull
        }
    }

    override def put(i: Int, o: scala.Any): Unit = {
        throw new Exception("This class is for output only")
    }
}

object SdcCombined {
	implicit val typeInfo = TypeInformation.of(classOf[SdcCombined])

	val EMPTY = SdcCombined(0, "", "", SdcDataInstant.EMPTY, None, None)

	def apply(): SdcCombined = EMPTY

	def apply(ts: Long, dslam: String, port: String, raw: String, ref: Array[Int]): SdcCombined = {
		val v = raw.split(",", -1)

		val i = if (v(ref(0)) != "")
			SdcDataInstant(
				v(ref(0)).equals("up"),
				v(ref(1)).equals("up"),
				v(ref(2)).toInt,
				v(ref(3)).toInt,
				v(ref(4)).toInt,
				v(ref(5)).toInt,
				Try(v(ref(6)).toShort).toOption,
				v(ref(7))
			) else SdcDataInstant.EMPTY

		val h = Try(SdcDataHistorical(
				v(ref(8)).toShort,
				v(ref(9)).toShort,
				v(ref(10)).toShort,
				v(ref(11)).toShort,
				v(ref(12)).toShort,
				v(ref(13)).toLong,
				v(ref(14)).toLong,
				v(ref(15)).toLong,
				v(ref(16)).toLong
			)).toOption

		new SdcCombined(ts, dslam, port, i, h, None)
	}

	def apply(raw: SdcCombined, enrich: EnrichmentData, correctedAttndrDS: Int, correctedAttndrUS: Int): SdcCombined =
		raw.copy(enrich = Some(SdcDataEnrichment(System.currentTimeMillis(),
			enrich.getOrElse(EnrichmentAttributeName.AVC, null).asInstanceOf[String],
			enrich.getOrElse(EnrichmentAttributeName.CPI, null).asInstanceOf[String],
			correctedAttndrDS, correctedAttndrUS)))

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
