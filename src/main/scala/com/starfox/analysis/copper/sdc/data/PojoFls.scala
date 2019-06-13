package com.starfox.analysis.copper.sdc.data

import java.sql.Timestamp
import java.time.Instant

/**
  * Created by Huyen on 5/9/18.
  */
class PojoFls(var avc_id: String,
			  var ntd_id: String,
			  var uni_prid: String,
			  var access_service_tech_type: String,
			  var metrics_date: Timestamp,
			  var avc_bandwidth_profile: String) {

	def this() = this("", "", "", "", Timestamp.from(Instant.MIN), "")

	override def toString: String = s"FlsRaw($avc_id,$ntd_id,$uni_prid,$access_service_tech_type)"
}