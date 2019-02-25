package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 5/9/18.
  */

case class RawAms(ts: Long, dslam: String, port: String, uni_prid: String) extends CopperLine

object RawAms {
	def apply(pojoAms: PojoAms) = {
		pojoAms.object_name.span(_ != ':') match {
			case (dslam, port) => new RawAms(pojoAms.metrics_date.toEpochMilli, dslam, port.drop(1), pojoAms.customer_id)
			case _ => new RawAms(Long.MinValue, "", "", "")
		}
	}
}