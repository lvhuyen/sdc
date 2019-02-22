package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.data.EnrichmentAttributeName._
import org.slf4j.LoggerFactory

//import scala.collection.immutable.Map

/**
  * Created by Huyen on 5/9/18.
  */


case class RawFls(ts: Long, uni_prid: String, data: Map[EnrichmentAttributeName, Any]) extends TemporalEvent

object RawFls {
	private val LOG = LoggerFactory.getLogger(classOf[RawFls])

	val UNKNOWN = RawFls(Long.MinValue, "", Map(EnrichmentAttributeName.TECH_TYPE -> TechType.NotSupported))

	private val regexTc4 = """.*D(\d+-)?(\d+)_U(\d+-)?(\d+)_Mbps_TC4.*""".r
//	private val techTypeShortener = Map(
//		"Fibre To The Node" -> "FTTN",
//		"Fibre To The Building" -> "FTTB"
//	)

	val unsupportedTechType = TechType.NotSupported

	def apply(pojoFls: PojoFls): RawFls = {
		try {
			val techType = TechType(pojoFls.access_service_tech_type)
			if (techType.equals(TechType.NotSupported))
				UNKNOWN
			else {
				val regexTc4(_, ds, _, us) = pojoFls.avc_bandwidth_profile
				RawFls(pojoFls.metrics_date.toEpochMilli,
					pojoFls.uni_prid,
					Map(
						EnrichmentAttributeName.AVC -> pojoFls.avc_id,
						EnrichmentAttributeName.CPI -> pojoFls.ntd_id,
						EnrichmentAttributeName.TC4_DS -> ds,
						EnrichmentAttributeName.TC4_US -> us,
						EnrichmentAttributeName.TECH_TYPE -> techType
					)
				)
			}
		} catch {
			case i: scala.MatchError => LOG.info(s"Unknown bandwidth profile: ${pojoFls.avc_bandwidth_profile} at ${pojoFls.avc_id}")
				UNKNOWN
		}
	}
}
