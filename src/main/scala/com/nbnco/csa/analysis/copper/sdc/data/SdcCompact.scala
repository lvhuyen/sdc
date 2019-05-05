package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName

import scala.util.Try

/**
  * Created by Huyen on 15/4/19.
  */

/**
  */

object SdcCompact {

    def apply(ts: Long, avc: String, source: collection.mutable.Map[String, AnyRef]): SdcCompact = {
        this(ts,
            source.getOrElse(FieldName.DSLAM, "").asInstanceOf[String],
            source.getOrElse(FieldName.PORT, "").asInstanceOf[String],
            avc,
            Try(source.get(FieldName.LPR).asInstanceOf[String].toShort).toOption,
            Try(source.get(FieldName.REINIT).asInstanceOf[String].toShort).toOption,
            Try(source.get(FieldName.UAS).asInstanceOf[String].toShort).toOption,
            source.getOrElse(FieldName.ATTNDRDS,"0").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.ATTNDRUS,"0").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.IFOPERSTATUS,"down").asInstanceOf[String].equals("up")
        )
    }

    def apply(raw: SdcCombined): SdcCompact = {
        this(raw.ts,
            raw.dslam,
            raw.port,
            raw.enrich.map(_.avc).getOrElse("UNKNOWN"),
            raw.dataH.map(_.lprFe),
            raw.dataH.map(_.reInit),
            raw.dataH.map(_.uas),
            raw.dataI.attndrDs,
            raw.dataI.attndrUs,
            raw.dataI.ifOperStatus)
    }
}

case class SdcCompact(ts: Long,
                      dslam: String,
                      port: String,
                      avc: String,
                      lpr: Option[Short],
                      reInit: Option[Short],
                      uas: Option[Short],
                      attndrDS: Int,
                      attndrUS: Int,
                      ifOperStatus: Boolean) extends TemporalEvent {

//    override def toString() = {
//        s"$ts,$dslam,$port,$avc,$lpr,$reInit,$uas,$attndrDS,$attndrUS,$ifOperStatus"
//    }

    override def equals(obj: Any): Boolean = {
        obj match {
            case o: SdcCompact => o.dslam == dslam && o.port == port
            case _ => false
        }
    }

    override def hashCode(): Int = s"$dslam$port".hashCode
}
