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
            source.get(FieldName.DSLAM).asInstanceOf[String],
            source.get(FieldName.PORT).asInstanceOf[String],
            Some(avc),
            Try(source.get(FieldName.LPR).asInstanceOf[String].toShort: JShort).toOption,
            Try(source.get(FieldName.REINIT).asInstanceOf[String].toShort: JShort).toOption,
            Try(source.get(FieldName.UAS).asInstanceOf[String].toShort: JShort).toOption,
            source.get(FieldName.ATTNDRDS).asInstanceOf[String].toInt,
            source.get(FieldName.ATTNDRUS).asInstanceOf[String].toInt,
            source.get(FieldName.IFOPERSTATUS).asInstanceOf[String].equals("up")
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




