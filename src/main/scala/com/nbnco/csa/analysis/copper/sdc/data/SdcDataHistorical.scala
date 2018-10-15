package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  *
  * @param ses          xdslLinePreviousIntervalSESCounter
  * @param uas          xdslLinePreviousIntervalUASCounter
  * @param lprFe        xdslFarEndLinePreviousIntervalLPRCounter
  * @param sesFe        xdslFarEndLinePreviousIntervalSESCounter
  * @param unCorrDtuDs  xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS
  * @param unCorrDtuUs  xdslChannelPreviousIntervalUnCorrDtuCounterUS
  * @param reInit       xdslLinePreviousIntervalReInitCounter
  * @param reTransUs    xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS
  * @param reTransDs    xdslChannelPreviousIntervalRetransmDtuCounterDS
  */
case class SdcDataHistorical(var ses: Int,
                             var uas: Int,
                             var lprFe: Int,
                             var sesFe: Int,
                             var unCorrDtuDs: Int,
                             var unCorrDtuUs: Int,
                             var reInit: Int,
                             var reTransUs : Int,
                             var reTransDs: Int
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
