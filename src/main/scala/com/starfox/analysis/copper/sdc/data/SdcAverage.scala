package com.starfox.analysis.copper.sdc.data

import org.json4s._
import org.json4s.jackson.Serialization.write

/**
  * Created by Huyen on 11/7/18.
  */
case class SdcAverage(ts: Long, dslam: String, port: String,
					  enrich: SdcDataEnrichment,
					  average_ds: Int, average_us: Int,
					  measurements_count: Long
                ) extends SdcEnrichedBase {

	def this() = this(0L, "", "", SdcDataEnrichment(), -1, -1, 0L)
//	override def toString =
//        s"$dslam,$port,${avc_id.getOrElse(dslam + ":" + port)},${cpi.getOrElse("N/A")},$data_time,$if_admin_status,$if_oper_status,$actual_ds,$actual_us,$attndr_ds,$attndr_us,${attenuation_ds},${user_mac_address.getOrElse("")},$measurements_count,$average_ds,$average_us"

	override def toString = {
		implicit val formats = DefaultFormats
		write(this)
	}

	override def toMap: Map[String, Any] = {
		Map (
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts,
			"averageDs" -> average_ds,
			"averageUs" -> average_us,
			"measurementsCount" -> measurements_count
		) ++ enrich.toMap
	}}

object SdcAverage {
}
