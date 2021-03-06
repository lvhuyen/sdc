package com.starfox.analysis.copper.sdc.data

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
case class SdcDataHistorical(ses: Short,
                             uas: Short,
                             lprFe: Short,
                             sesFe: Short,
							 reInit: Short,
                             unCorrDtuDs: Long,
                             unCorrDtuUs: Long,
                             reTransUs : Long,
                             reTransDs: Long
                            ) {
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
//	val EMPTY = SdcDataHistorical(null, null, null, null, null, null, null, null, null)
	val EMPTY = SdcDataHistorical(-1: Short, -1: Short, -1: Short, -1: Short, -1: Short, -1L, -1L, -1L, -1L)
	def apply(): SdcDataHistorical = EMPTY
}
