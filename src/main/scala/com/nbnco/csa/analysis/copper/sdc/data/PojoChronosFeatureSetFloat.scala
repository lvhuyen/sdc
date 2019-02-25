package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 21/02/19.
  */
class PojoChronosFeatureSetFloat(var id: String,
								 var id_type: String,
								 var value: Float) {

	def this() = this("", "", 0.0f)

	override def toString: String = s"FlsRaw($id,$id_type,$value)"
}