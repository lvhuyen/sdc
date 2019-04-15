package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName

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
            source.getOrElse(FieldName.LPR, "-1").asInstanceOf[String].toShort,
            source.getOrElse(FieldName.REINIT, "-1").asInstanceOf[String].toShort,
            source.getOrElse(FieldName.UAS, "-1").asInstanceOf[String].toShort,
            source.getOrElse(FieldName.ATTNDRDS, "-1").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.ATTNDRUS, "-1").asInstanceOf[String].toInt,
            source.getOrElse(FieldName.IFOPERSTATUS, "").asInstanceOf[String].equals("up")
        )
    }

    def apply(raw: SdcCombined): SdcCompact = {
        this(raw.ts,
            raw.dslam,
            raw.port,
            raw.enrich.avcId,
            raw.dataH.lprFe.toShort,
            raw.dataH.reInit.toShort,
            raw.dataH.uas.toShort,
            raw.dataI.attndr_ds,
            raw.dataI.attndr_us,
            raw.dataI.if_oper_status.equals("up"))
    }
}

case class SdcCompact(ts: Long,
                      dslam: String,
                      port: String,
                      avc: String,
                      lpr: Short,
                      reInit: Short,
                      uas: Short,
                      attndrDS: Int,
                      attndrUS: Int,
                      ifOperStatus: Boolean) {

    override def toString() = {
        s"$ts,$dslam,$port,$avc,$lpr,$reInit,$uas,$attndrDS,$attndrUS,$ifOperStatus"
    }
}




