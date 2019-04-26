package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class MyClass(ts: Long,
				   s1: String,
				   s2: String,
				   i1: Integer,
				   i2: Integer) {
	def toMap: Map[String, Any] = {
		Map (
			"enrich_timestamp" -> System.currentTimeMillis(),
			"avc_id" -> s1,
			"cpi" -> s2,
			"correctedAttndrDs" -> i1,
			"correctedAttndrUs" -> i2
		)
	}
}

object MyClass {
	val EMPTY = MyClass(0L, null, null, null, 0)

	def apply(): MyClass = EMPTY
}