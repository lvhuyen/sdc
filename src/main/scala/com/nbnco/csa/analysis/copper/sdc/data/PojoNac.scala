package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 21/02/19.
  */
class PojoNac(var dslam: String,
			  var port: String,
			  var max_additional_noise_margin_us: String,
			  var max_additional_noise_margin_ds: String,
			  var dpbo_profile_name: String,
			  var rtx_attainable_net_data_rate_ds: String) {

	def this() = this("", "", "", "", "", "")

	override def toString: String =
		s"PojoNac:${dslam},${port},${max_additional_noise_margin_us},${max_additional_noise_margin_ds},${dpbo_profile_name},${rtx_attainable_net_data_rate_ds}"
}