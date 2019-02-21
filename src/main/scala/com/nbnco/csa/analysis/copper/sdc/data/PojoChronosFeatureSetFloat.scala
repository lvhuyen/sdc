package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 21/02/19.
  */
class PojoChronosFeatureSetFloat(var id: String,
								 var idType: String,
								 var value: Float,
								 var attrname: String) {

	def this() = this("", "", 0.0f, "")

	override def toString: String = s"FlsRaw($id,$idType,$value,$attrname)"
}