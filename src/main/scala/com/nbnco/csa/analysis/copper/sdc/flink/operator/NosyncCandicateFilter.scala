package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 15/4/19.
  */

/**
  * This RichCoFlatMapFunction is used to enrich a SdcRaw object with AVC_ID and CPI
  */
class NosyncCandicateFilter extends RichCoFlatMapFunction[SdcCombined, ((String, String), Boolean), SdcCompact] {
	val candidateStateDescriptor = new ValueStateDescriptor[Boolean]("NoSyncEnabled", classOf[Boolean])

	override def flatMap1(in1: SdcCombined, collector: Collector[SdcCompact]): Unit = {
		val cachedState = getRuntimeContext.getState(candidateStateDescriptor)
		if (cachedState.value)
			collector.collect(SdcCompact(in1))
	}

	override def flatMap2(in2: ((String, String), Boolean), collector: Collector[SdcCompact]): Unit = {
		val cachedState = getRuntimeContext.getState(candidateStateDescriptor).update(in2._2)
	}
}
