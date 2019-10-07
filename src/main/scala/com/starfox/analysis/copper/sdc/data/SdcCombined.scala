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
case class SdcCombined(var ts: Long, var dslam: String, var port: String,
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
            case 3 => dataI.ifAdminStatus: String
            case 4 => dataI.ifOperStatus: String
            case 5 => dataI.actualDs: Integer
            case 6 => dataI.actualUs: Integer
            case 7 => dataI.attndrDs: Integer
            case 8 => dataI.attndrUs: Integer
            case 9 => dataI.attenuationDs.map(_/10.0f: JFloat).orNull
            case 10 => dataI.userMacAddress.orNull
            case 11 => dataH.map(_.ses: JShort).orNull
            case 12 => dataH.map(_.uas: JShort).orNull
            case 13 => dataH.map(_.lprFe: JShort).orNull
            case 14 => dataH.map(_.sesFe: JShort).orNull
			case 15 => dataH.map(_.reInit: JShort).orNull
            case 16 => dataH.map(_.unCorrDtuDs: JLong).orNull
            case 17 => dataH.map(_.unCorrDtuUs: JLong).orNull
            case 18 => dataH.map(_.reTransDs: JLong).orNull
            case 19 => dataH.map(_.reTransUs: JLong).orNull
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
				v(ref(0)),
				v(ref(1)),
				v(ref(2)).toInt,
				v(ref(3)).toInt,
				v(ref(4)).toInt,
				v(ref(5)).toInt,
				Try(v(ref(6)).toShort).toOption,
				Some(v(ref(7)))
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
		"xdslChannelPreviousIntervalRetransmDtuCounterDS",
		"xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS"
	)


	lazy val SCHEMA: Schema = {
		org.apache.avro.SchemaBuilder
				.record("SdcCombined").namespace("com.nbnco")
				.fields()
				.name("ts").`type`("long").noDefault()
				.name("dslam").`type`("string").noDefault()
				.name("port").`type`("string").noDefault()
				.name("if_admin_status").`type`("string").noDefault()
				.name("if_oper_status").`type`("string").noDefault()
				.name("actual_ds").`type`("int").noDefault()
				.name("actual_us").`type`("int").noDefault()
				.name("attndr_ds").`type`("int").noDefault()
				.name("attndr_us").`type`("int").noDefault()
				.name("attenuation_ds").`type`().nullable().floatType().noDefault()
				.name("mac_address").`type`().nullable().stringType().noDefault()
				.name("ses").`type`().nullable().intType().noDefault()
				.name("uas").`type`().nullable().intType().noDefault()
				.name("lpr_fe").`type`().nullable().intType().noDefault()
				.name("ses_fe").`type`().nullable().intType().noDefault()
				.name("re_init").`type`().nullable().intType().noDefault()
				.name("un_corr_dtu_ds").`type`().nullable().longType().noDefault()
				.name("un_corr_dtu_us").`type`().nullable().longType().noDefault()
				.name("re_trans_ds").`type`().nullable().longType().noDefault()
				.name("re_trans_us").`type`().nullable().longType().noDefault()
				.name("ts_enrich").`type`().nullable().longType().noDefault()
				.name("avc").`type`().nullable().stringType().noDefault()
				.name("cpi").`type`().nullable().stringType().noDefault()
				.name("corrected_attndr_ds").`type`().nullable().intType().noDefault()
				.name("corrected_attndr_us").`type`().nullable().intType().noDefault()
				.endRecord()
	}
}
