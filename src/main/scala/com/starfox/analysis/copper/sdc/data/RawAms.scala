package com.starfox.analysis.copper.sdc.data

/**
  * Created by Huyen on 5/9/18.
  */

case class RawAms(var ts: Long, var dslam: String, var port: String, var uni_prid: String) extends CopperLine {
	def this() = this(Long.MinValue, "", "", "")
}

object RawAms {
	def apply(pojoAms: PojoAms) = {
		pojoAms.object_name.span(_ != ':') match {
			case (dslam, port) => new RawAms(pojoAms.metrics_date.getTime, dslam, port.drop(1), pojoAms.customer_id)
			case _ => new RawAms(Long.MinValue, "", "", "")
		}
	}
}