package com.nbnco.csa.analysis.copper.sdc

/**
  * Created by Huyen on 23/9/18.
  */
package object data {
	/***
		* Each Port_Pattern is one tuple of
		*   - a regex to parse the raw port string, and
		*   - a function of (r,s,lt,p) to reconstruct the port to desired format.
		*     This function would help in case there is a need to manipulate value of the raw numbers
		*/

	val PORT_PATTERNS = Array (
		("""R(\d)\.S(\d)\.LT(\d{1,2})\.P(\d{1,2})""".r,
			(r: String, s: String, lt: String, p: String) => s"R$r.S$s.LT$lt.P$p"),

		("""\/.*\/rackNr=(\d)\/subrackNr=(\d)\/slotNr=(\d{1,2})\/portNr=(\d{1,2}).*""".r,
			(r: String, s: String, lt: String, p: String) => s"R$r.S$s.LT${lt.toInt - 2}.P$p")
	)
	val PORT_PATTERN_UNKNOWN = -1
	val PORT_PATTERN_DEFAULT = 0

	object TechType extends Enumeration {
		type TechType = Value
		val FTTN, FTTB, FTTC, Unknown = Value

		def apply(techType: String): Value = {
			techType match {
				case "Fibre To The Node" => FTTN
				case "Fibre To The Building" => FTTB
				case "Fibre To The Curb" => FTTC
				case _ => Unknown
			}
		}
	}

	object EnrichmentAttributeName extends Enumeration {
		type EnrichmentAttributeName = Value
		val AVC, CPI, TECH_TYPE, TC4, DPBO_PROFILE, ATTEN365, NOISE_MARGIN_DS, NOISE_MARGIN_US = Value
	}

	type EnrichmentData = Map[EnrichmentAttributeName.Value, Any]


	//	object Tc4 extends Enumeration {
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
