package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 19/10/18.
  */
case class DslamMetadata (isInstant: Boolean, name: String, metricsTime: Long, columns: String, relativePath: String, fileTime: Long, processingTime: Long)

object DslamMetadata {
	def toMap(dslam: DslamMetadata): Map[String, Any] = {
		Map (
			"isInstant" -> dslam.isInstant,
			"dslam" -> dslam.name,
			"metricsTime" -> dslam.metricsTime,
			"fileTime" -> dslam.fileTime,
			"processingTime" -> dslam.processingTime,
			"path" -> dslam.relativePath
		)
	}
}
