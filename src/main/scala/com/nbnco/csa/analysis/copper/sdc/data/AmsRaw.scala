package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 5/9/18.
  */
class AmsRaw (var object_name: String, var customer_id: String, var metrics_date: java.time.Instant) {
	def this () = this("", "", java.time.Instant.MIN)

//	override def toString: String = s"FlsRaw($avc_id,$ntd_id,$uni_prid,$access_service_tech_type)"
}

object AmsRaw {
	def unapply(arg: AmsRaw): Option[(String, String)] = {
		arg.object_name.span(_ != ':') match {
			case (dslam, port) => Some ((dslam, port.drop(1)))
			case _ => None
		}
	}
}
