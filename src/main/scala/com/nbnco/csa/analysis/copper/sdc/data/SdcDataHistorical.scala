package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  *
  * @param ses          xdslLinePreviousIntervalSESCounter				// SES = Severely Errored Seconds
  * @param uas          xdslLinePreviousIntervalUASCounter				// UAS = UnAvailable Seconds
  * @param lprFe        xdslFarEndLinePreviousIntervalLPRCounter		// LPR = Loss of Power
  * @param sesFe        xdslFarEndLinePreviousIntervalSESCounter
  * @param reInit       xdslLinePreviousIntervalReInitCounter
  * @param unCorrDtuDs  xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS		// UnCorrectable DTU
  * @param unCorrDtuUs  xdslChannelPreviousIntervalUnCorrDtuCounterUS
  * @param reTransUs    xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS
  * @param reTransDs    xdslChannelPreviousIntervalRetransmDtuCounterDS
  *                     Drop-out = reInit - LPR
  */
case class SdcDataHistorical(var ses: JShort,
                             var uas: JShort,
                             var lprFe: JShort,
                             var sesFe: JShort,
							 var reInit: JShort,
                             var unCorrDtuDs: JLong,
                             var unCorrDtuUs: JLong,
                             var reTransUs : JLong,
                             var reTransDs: JLong
                            ) extends SdcDataBase {
	def toMap: Map[String, Any] = {
		Map (
			"xdslLinePreviousIntervalSESCounter" -> ses,
			"xdslLinePreviousIntervalUASCounter" -> uas,
			"xdslFarEndLinePreviousIntervalLPRCounter" -> lprFe,
			"xdslFarEndLinePreviousIntervalSESCounter" -> sesFe,
			"xdslLinePreviousIntervalReInitCounter" -> reInit,
			"xdslFarEndChannelPreviousIntervalUnCorrDtuCounterDS" -> unCorrDtuDs,
			"xdslChannelPreviousIntervalUnCorrDtuCounterUS" -> unCorrDtuUs,
			"xdslFarEndChannelPreviousIntervalRetransmDtuCounterUS" -> reTransUs,
			"xdslChannelPreviousIntervalRetransmDtuCounterDS" -> reTransDs
		)
	}
}
object SdcDataHistorical {
	val EMPTY = SdcDataHistorical(null, null, null, null, null, null, null, null, null)
//	val EMPTY = SdcDataHistorical(java.lang.Short.MIN_VALUE,java.lang.Short.MIN_VALUE,java.lang.Short.MIN_VALUE,java.lang.Short.MIN_VALUE,java.lang.Short.MIN_VALUE, -1L, -1L, -1L, -1L)
	def apply(): SdcDataHistorical = EMPTY

//	def parse(raw: String, ref: Array[Int]): SdcDataBase = {
//		val v = raw.split(",")
//		SdcDataHistorical(v(ref(0)), v(ref(1)), v(ref(2)).toInt, v(ref(3)).toInt, v(ref(4)).toInt, v(ref(5)).toInt, v(ref(6)), v.lift(ref(7)).getOrElse(""))
//	}
}
