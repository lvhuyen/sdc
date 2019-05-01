package com.nbnco.csa.analysis.copper.sdc.data

import java.sql.Timestamp
import java.time.LocalDateTime

/**
  * Created by Huyen on 21/02/19.
  */
class PojoChronosFeatureSetFloat(var metrics_timestamp: Timestamp,
								 var id: String,
								 var id_type: String,
								 var value: Float) {

	def this() = this(PojoChronosFeatureSetFloat.MIN_TIME, "", "", 0.0f)

	override def toString: String = s"FlsRaw($id,$id_type,$value)"
}

object PojoChronosFeatureSetFloat {
	val MIN_TIME = Timestamp.valueOf(LocalDateTime.MIN)
}