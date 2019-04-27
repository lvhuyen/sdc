package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName

import scala.util.Try

/**
  * Created by Huyen on 15/4/19.
  */

/**
  */

object SdcCompact {

    def apply(ts: JLong, avc: String, source: collection.mutable.Map[String, AnyRef]): SdcCompact = {
        this(ts,
            source.getOrElse(FieldName.DSLAM, "").asInstanceOf[String],
            source.getOrElse(FieldName.PORT, "").asInstanceOf[String],
            Some(avc),
            source.get(FieldName.LPR).map(_.asInstanceOf[String].toShort),
            source.get(FieldName.REINIT).map(_.asInstanceOf[String].toShort),
            source.get(FieldName.UAS).map(_.asInstanceOf[String].toShort),
            source.getOrElse(FieldName.ATTNDRDS,"0").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.ATTNDRUS,"0").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.IFOPERSTATUS,"down").asInstanceOf[String].equals("up")
        )
    }

    def apply(raw: SdcCombined): SdcCompact = {
        this(raw.ts,
            raw.dslam,
            raw.port,
            raw.enrich.map(_.avc),
            raw.dataH.map(_.lprFe),
            raw.dataH.map(_.reInit),
            raw.dataH.map(_.uas),
            raw.dataI.attndrDs,
            raw.dataI.attndrUs,
            raw.dataI.ifOperStatus)
    }
}

case class SdcCompact(ts: JLong,
                      dslam: String,
                      port: String,
                      avc: Option[String],
                      lpr: Option[JShort],
                      reInit: Option[JShort],
                      uas: Option[JShort],
                      attndrDS: JInt,
                      attndrUS: JInt,
                      ifOperStatus: JBool) {

    override def toString() = {
        s"$ts,$dslam,$port,$avc,$lpr,$reInit,$uas,$attndrDS,$attndrUS,$ifOperStatus"
    }
}




