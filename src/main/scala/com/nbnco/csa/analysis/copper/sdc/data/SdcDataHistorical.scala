package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  *
  * @param ses          xdslLinePreviousIntervalSESCounter				// SES = Severely Errored Seconds
  * @param uas          xdslLinePreviousIntervalUASCounter				// UAS = UnAvailable Seconds
  * @param lprFe        xdslFarEndLinePreviousIntervalLPRCounter		// LPR = Loss of Power
  * @param sesFe        xdslFarEndLinePreviousIntervalSESCounter
  * @param unCorrDtuDs  xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS		// UnCorrectable DTU
  * @param unCorrDtuUs  xdslChannelPreviousIntervalUnCorrDtuCounterUS
  * @param reInit       xdslLinePreviousIntervalReInitCounter
  * @param reTransUs    xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS
  * @param reTransDs    xdslChannelPreviousIntervalRetransmDtuCounterDS
	*                     Drop-out = reInit = LPR
  */
case class SdcDataHistorical(var ses: Long,
                             var uas: Long,
                             var lprFe: Long,
                             var sesFe: Long,
                             var unCorrDtuDs: Long,
                             var unCorrDtuUs: Long,
                             var reInit: Long,
                             var reTransUs : Long,
                             var reTransDs: Long
                            ) extends SdcDataBase {
	def toMap: Map[String, Any] = {
		Map (
			"xdslLinePreviousIntervalSESCounter" -> ses,
			"xdslLinePreviousIntervalUASCounter" -> uas,
			"xdslFarEndLinePreviousIntervalLPRCounter" -> lprFe,
			"xdslFarEndLinePreviousIntervalSESCounter" -> sesFe,
			"xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS" -> unCorrDtuDs,
			"xdslChannelPreviousIntervalUnCorrDtuCounterUS" -> unCorrDtuUs,
			"xdslLinePreviousIntervalReInitCounter" -> reInit,
			"xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS" -> reTransUs,
			"xdslChannelPreviousIntervalRetransmDtuCounterDS" -> reTransDs
		)
	}
}
object SdcDataHistorical {
	def apply(): SdcDataHistorical = SdcDataHistorical(-1, -1, -1, -1, -1, -1, -1, -1, -1)
//	def parse(raw: String, ref: Array[Int]): SdcDataBase = {
//		val v = raw.split(",")
//		SdcDataHistorical(v(ref(0)), v(ref(1)), v(ref(2)).toInt, v(ref(3)).toInt, v(ref(4)).toInt, v(ref(5)).toInt, v(ref(6)), v.lift(ref(7)).getOrElse(""))
//	}
}
