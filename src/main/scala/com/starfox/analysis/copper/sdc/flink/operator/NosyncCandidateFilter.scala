package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 15/4/19.
  */

/**
  * This function filters the 1st input stream SdcCombined using data from 2nd stream ((dslam, port), flag)
  * The output is a stream of SdcCompact (a trimmed version of SdcCombined to save memory space)
  */
class NosyncCandidateFilter extends RichCoFlatMapFunction[SdcCombined, ((String, String), Boolean), SdcCompact] {
	val candidateStateDescriptor = new ValueStateDescriptor[Boolean]("NoSyncEnabled", classOf[Boolean])
	candidateStateDescriptor.setQueryable("NoSyncMonitorEnabled")

	override def flatMap1(in1: SdcCombined, collector: Collector[SdcCompact]): Unit = {
		val cachedState = getRuntimeContext.getState(candidateStateDescriptor)
		if (cachedState.value)
			collector.collect(SdcCompact(in1))
	}

	override def flatMap2(in2: ((String, String), Boolean), collector: Collector[SdcCompact]): Unit = {
		getRuntimeContext.getState(candidateStateDescriptor).update(in2._2)
	}
}
