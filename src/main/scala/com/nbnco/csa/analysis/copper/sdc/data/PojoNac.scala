package com.nbnco.csa.analysis.copper.sdc.data

import java.sql.Timestamp
import java.time.LocalDateTime

/**
  * Created by Huyen on 21/02/19.
  */
class PojoNac(var metrics_timestamp: Timestamp,
			  var dslam: String,
			  var port: String,
			  var max_additional_noise_margin_us: String,
			  var max_additional_noise_margin_ds: String,
			  var dpbo_profile_name: String,
			  var rtx_attainable_net_data_rate_ds: String) {

	def this() = this(PojoNac.MIN_TIME, "", "", "", "", "", "")

	override def toString: String =
		s"PojoNac:${metrics_timestamp},${dslam},${port},${max_additional_noise_margin_us},${max_additional_noise_margin_ds},${dpbo_profile_name},${rtx_attainable_net_data_rate_ds}"
}

object PojoNac {
	val MIN_TIME = Timestamp.valueOf(LocalDateTime.MIN)
}