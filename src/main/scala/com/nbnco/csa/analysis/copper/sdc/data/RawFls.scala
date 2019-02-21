package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.data.TechType.TechType
import com.nbnco.csa.analysis.copper.sdc.data.EnrichmentAttributeName._

//import scala.collection.immutable.Map

/**
  * Created by Huyen on 5/9/18.
  */


case class RawFls(ts: Long, uni_prid: String, data: Map[EnrichmentAttributeName, Any]) extends TemporalEvent

object RawFls {
	private val regexTc4 = """.*D([\d-]*)_U([\d-]*)_Mbps_TC4.*""".r

	def apply(pojoFls: PojoFls): RawFls = {
		new RawFls (pojoFls.metrics_date.toEpochMilli,
			pojoFls.uni_prid,
			Map(
				EnrichmentAttributeName.AVC -> pojoFls.avc_id,
				EnrichmentAttributeName.CPI -> pojoFls.ntd_id,
				EnrichmentAttributeName.TC4 -> regexTc4.replaceFirstIn(pojoFls.avc_bandwidth_profile, "$1/$2"),
				EnrichmentAttributeName.TECH_TYPE -> TechType(pojoFls.access_service_tech_type)
			)
		)
	}
}
