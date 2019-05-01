package com.nbnco.csa.analysis.copper.sdc.flink.cep

import java.util

import com.nbnco.csa.analysis.copper.sdc.data.{SdcCompact}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.conditions.{Context => CepContext}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.util.Collector


object NoSync {
	private def accSum(f: SdcCompact => Long, keys: Seq[String], currentEvent: SdcCompact, ctx: CepContext[SdcCompact]): Long = {
		keys.map(key => ctx.getEventsForPattern(key).map(f).sum).sum + f(currentEvent)
	}

	def apply(input: KeyedStream[SdcCompact, _],
			  numberOfReInitMeasurements: Int,
			  numberOfOperUpMeasurements: Int,
			  maxReInitCount: Int): DataStream[SdcCompact] = {

		val patternLineIsUp = Pattern.begin[SdcCompact]("reInitCheck")
				.where((value: SdcCompact, ctx: CepContext[SdcCompact]) => accSum(_.reInit, Seq("reInitCheck"), value, ctx) < maxReInitCount)
				.times(numberOfReInitMeasurements - numberOfOperUpMeasurements).consecutive()
        		.next("statusCheck")
				.where((value: SdcCompact, ctx: CepContext[SdcCompact]) =>
					accSum(_.reInit, Seq("reInitCheck", "statusCheck"), value, ctx) < maxReInitCount && value.ifOperStatus )
				.times(numberOfOperUpMeasurements).consecutive()

		collectPattern(input, patternLineIsUp)
	}

	private def collectPattern(inputStream: KeyedStream[SdcCompact, _], pattern: Pattern[SdcCompact, SdcCompact]): DataStream[SdcCompact] =
		CEP.pattern(inputStream, pattern)
				.process((map: util.Map[String, util.List[SdcCompact]], _, collector: Collector[SdcCompact]) => {
					val records = map.get("statusCheck")
					collector.collect(records.get(records.size() - 1))
				})
}
