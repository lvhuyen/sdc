package com.starfox.analysis.copper.sdc.data

/**
  * Created by Huyen on 15/8/18.
  */
//
//abstract class SdcRecord () {
//	var ts: Long = 0L
//	var dslam: String = _
//	var port: String = _
//	def toMap: Map[String, Any] = {
//		Map (
//			"dslam" -> dslam,
//			"port" -> port,
//			"metrics_timestamp" -> ts
//		)
//	}
//}


trait CopperLine extends TemporalEvent {
	var  dslam: String
	var  port: String

	def toMap: Map[String, Any] = {
		Map (
			"dslam" -> dslam,
			"port" -> port,
			"metrics_timestamp" -> ts
		)
	}
}
