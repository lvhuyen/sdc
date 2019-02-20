package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 5/9/18.
  */
class FlsRaw (var avc_id: String,
			  var ntd_id: String,
			  var uni_prid: String,
			  var access_service_tech_type: String,
			  var metrics_date: java.time.Instant,
			  var avc_bandwidth_profile: String) {

	def this() = this("", "", "", "", java.time.Instant.MIN, "")

	override def toString: String = s"FlsRaw($avc_id,$ntd_id,$uni_prid,$access_service_tech_type)"
}
