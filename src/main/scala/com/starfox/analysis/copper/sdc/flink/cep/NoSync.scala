package com.starfox.analysis.copper.sdc.flink.cep

import java.util

import com.starfox.analysis.copper.sdc.data.SdcCompact
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.conditions.{Context => CepContext}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.util.Collector


object NoSync {
	private def accSum(f: SdcCompact => Short, keys: Seq[String], currentEvent: SdcCompact, ctx: CepContext[SdcCompact]): Long = {
		keys.map(key => ctx.getEventsForPattern(key).map(f).sum).sum + f(currentEvent)
	}

	def apply(input: KeyedStream[SdcCompact, _],
			  numberOfReInitMeasurements: Int,
			  numberOfOperUpMeasurements: Int,
			  maxReInitCount: Int,
			  maxUnAvailableSecond: Int): DataStream[SdcCompact] = {

		val patternLineIsUp = Pattern.begin[SdcCompact]("reInitCheck")
				.where((value: SdcCompact, ctx: CepContext[SdcCompact]) => accSum(_.reInit.getOrElse(0: Short), Seq("reInitCheck"), value, ctx) < maxReInitCount)
				.times(numberOfReInitMeasurements - numberOfOperUpMeasurements).consecutive()
        		.next("statusCheck")
				.where((value: SdcCompact, ctx: CepContext[SdcCompact]) =>
					value.ifOperStatus &&
							accSum(_.reInit.getOrElse(0: Short), Seq("reInitCheck", "statusCheck"), value, ctx) < maxReInitCount &&
							accSum(_.uas.getOrElse(0: Short), Seq("statusCheck"), value, ctx) < maxUnAvailableSecond)
				.times(numberOfOperUpMeasurements).consecutive()

		collectPattern(input, patternLineIsUp)
	}

	private def collectPattern(inputStream: KeyedStream[SdcCompact, _], pattern: Pattern[SdcCompact, SdcCompact]): DataStream[SdcCompact] =
		CEP.pattern(inputStream, pattern)
				.process((map: util.Map[String, util.List[SdcCompact]], ctx: PatternProcessFunction.Context, collector: Collector[SdcCompact]) => {
					val records = map.get("statusCheck")
					collector.collect(records.get(records.size() - 1))
				})
}
