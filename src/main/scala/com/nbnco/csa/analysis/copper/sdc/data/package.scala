package com.nbnco.csa.analysis.copper.sdc

import com.nbnco.csa.analysis.copper.sdc.utils.{InvalidDataException, InvalidPortFormatException}
import org.apache.flink.core.fs.FileInputSplit

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

/**
  * Created by Huyen on 23/9/18.
  */
package object data {
	type EnrichmentData = Map[EnrichmentAttributeName.Value, Any]
	type JFloat = java.lang.Float
	type JBool = java.lang.Boolean
	type JInt = java.lang.Integer
	type JShort = java.lang.Short
	type JLong = java.lang.Long

	object DslamType {
		val DSLAM_NONE: JInt = 0
		val DSLAM_HISTORICAL: JInt = 1
		val DSLAM_INSTANT: JInt = 2
		val DSLAM_COMBINED: JInt = 3
	}

	object EnrichmentAttributeName extends Enumeration {
		type EnrichmentAttributeName = Value
		val AVC, CPI, TECH_TYPE, TC4_DS, TC4_US, DPBO_PROFILE, ATTEN365, NOISE_MARGIN_DS, NOISE_MARGIN_US = Value
		val PCTLS_CURRENT_DS, PCTLS_CURRENT_US, PCTLS_CORRECTED_DS, PCTLS_CORRECTED_US = Value
	}

	object TechType extends Enumeration {
		type TechType = Value
		val FTTN, FTTB, NotSupported = Value

		def apply(techType: String): Value = {
			techType match {
				case "Fibre To The Node" => FTTN
				case "Fibre To The Building" => FTTB
				case _ => NotSupported
			}
		}
	}

	/***
	  * This function merges two Map[String, String] into one basing on the key (port)
	  * When one port exist in one map but not the other,
	  * 	padding commas are added to ensure that the output is still a valid CSV string
	  * 	The referral headers are used to identify how many padding commas should be added
	  * @param c1: 1st headers. If 1st headers = "" then return (c2, r2)
	  * @param r1: 1st Map
	  * @param c2: 2nd headers
	  * @param r2: 2nd Map
	  * @return: a tuple2 of (new headers, new Map)
	  */
	def mergeCsv(c1: String, r1: Map[String, String], c2: String, r2: Map[String, String]): Map[String, String] =
		{
			val d1blank = c1.filter(_.equals(','))
			val d2blank = c2.filter(_.equals(','))

			(r1.keySet ++ r2.keySet).map(k => (k, s"${r1.getOrElse(k, d1blank)},${r2.getOrElse(k, d2blank)}")).toMap
		}

//		object Tc4 extends Enumeration {
//		type Tc4 = Value
//		val P12_1, P25_5, P25_10, P50_20, P100_40, Tc4Unknown = Value
//
//		private val regexTc4 = """.*D([\d-]*)_U([\d-]*)_Mbps_TC4.*""".r
//
//		def apply(techType: String): Value = {
//			val regexTc4(up, down) = techType
//			techType match {
//				case "Fibre To The Node" => FTTN
//				case "Fibre To The Building" => FTTB
//				case "Fibre To The Curb" => FTTC
//				case _ => Tc4Unknown
//			}
//		}
//	}
}
