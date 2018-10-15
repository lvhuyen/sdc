package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/7/18.
  */

/**
  * @param dslam
  * @param ts               Timestamp: The time when data is measured. To be used as event_time
  * @param port
  */
case class SdcRawInstant(ts: Long, dslam: String, port: String,
                         data: SdcDataInstant
                   ) extends SdcRawBase  {

    def this () = this (0L, "", "", SdcDataInstant())

    def enrich(enrich: SdcDataEnrichment): SdcEnrichedInstant= {
        SdcEnrichedInstant(this, enrich)
    }
    override def toMap: Map[String, Any] = {
        Map (
            "dslam" -> dslam,
            "port" -> port,
            "metrics_timestamp" -> ts
        ) ++ data.toMap
    }
}

object SdcRawInstant extends SdcParser[SdcRawInstant] {
    override def parse(ts: Long, dslam: String, port: String, raw: String, ref: Array[Int]): SdcRawInstant = {
        val v = raw.split(',')
        new SdcRawInstant(ts, dslam, port,
            SdcDataInstant(v(ref(0)), v(ref(1)), v(ref(2)).toInt, v(ref(3)).toInt, v(ref(4)).toInt, v(ref(5)).toInt,
                if (v(ref(6)).equals("")) null else v(ref(6)).toInt,
                v.lift(ref(7)).orNull))
    }
}
