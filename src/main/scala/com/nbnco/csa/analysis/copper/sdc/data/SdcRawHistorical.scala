package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 11/8/18.
  */

/**
  *
  * @param ts    The event time
  * @param dslam        Name of the DSLAM
  * @param port         Port on the DSLAM
  */
case class SdcRawHistorical(ts: Long, dslam: String, port: String,
                            data: SdcDataHistorical
                      ) extends SdcRawBase {

	def enrich(enrich: EnrichmentData): SdcEnrichedHistorical = {
		SdcEnrichedHistorical(this, enrich)
	}

	override def toMap: Map[String, Any] = {
		Map (
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts
		) ++ data.toMap
	}
}

object SdcRawHistorical extends SdcParser[SdcRawHistorical] {
	override def parse(ts: Long, dslam: String, port: String, raw: String, ref: Array[Int]): SdcRawHistorical = {
		val v = raw.split(',')
		new SdcRawHistorical(ts, dslam, port, SdcDataHistorical(v(ref(0)).toLong, v(ref(1)).toLong, v(ref(2)).toLong, v(ref(3)).toLong, v(ref(4)).toLong, v(ref(5)).toLong, v(ref(6)).toLong, v(ref(7)).toLong, v(ref(8)).toLong))
	}
}