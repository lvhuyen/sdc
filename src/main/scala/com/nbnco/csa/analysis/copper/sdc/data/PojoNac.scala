package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 21/02/19.
  */
class PojoNac(var dslam: String,
			  var port: String,
			  var max_additional_noise_margin_us: String,
			  var max_additional_noise_margin_ds: String,
			  var dpbo_profile_name: String) {

	def this() = this("", "", "", "", "")

	override def toString: String = s"FlsRaw($dslam,$port,$max_additional_noise_margin_ds,$max_additional_noise_margin_us,$dpbo_profile_name)"
}