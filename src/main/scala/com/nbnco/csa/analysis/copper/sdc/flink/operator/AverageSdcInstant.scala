package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data.{SdcAverage, SdcEnrichedBase, SdcEnrichedInstant, SdcDataEnrichment}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Huyen on 24/9/18.
  */
object AverageSdcInstant {
	class Accumulator(var ts: Long,
	                  var enrich: SdcDataEnrichment,
	                  var sum_ds: Int, var sum_us: Int,
	                  var measurements_count: Int) {
		def this() = this (0L, SdcDataEnrichment(), 0, 0, 0)
		def this(a: Accumulator) = this (a.ts, a.enrich, a.sum_ds, a.sum_us, a.measurements_count)
	}
	private val triggeredStateDescriptor = new ValueStateDescriptor[Boolean]("triggered", createTypeInformation[Boolean])

	class AggIncremental extends AggregateFunction[SdcEnrichedInstant, Accumulator, Accumulator] {
		override def createAccumulator() = new Accumulator()

		override def add(in: SdcEnrichedInstant, acc: Accumulator): Accumulator = {
			if (acc.ts < in.ts) acc.ts = in.ts
			if (acc.enrich.tsEnrich < in.enrich.tsEnrich)
				acc.enrich = in.enrich
			acc.sum_ds += in.data.attndr_ds
			acc.sum_us += in.data.attndr_us
			acc.measurements_count += 1
			acc
		}

		override def merge(acc: Accumulator, acc2: Accumulator): Accumulator = {
			if (acc.ts < acc2.ts) acc.ts = acc2.ts
			if (acc.enrich.tsEnrich < acc2.enrich.tsEnrich)
				acc.enrich = acc2.enrich
			acc.sum_ds += acc2.sum_ds
			acc.sum_us += acc2.sum_us
			acc.measurements_count += acc2.measurements_count
			acc
		}

		override def getResult(acc: Accumulator): Accumulator = acc
	}

	class AggFinal extends ProcessWindowFunction[Accumulator, SdcAverage, (String, String), TimeWindow] {
		override def process(key: (String, String), context: Context, elements: Iterable[Accumulator], out: Collector[SdcAverage]): Unit = {
			val acc = elements.iterator.next()
			out.collect(new SdcAverage(acc.ts, key._1, key._2,
				acc.enrich,
				acc.sum_ds / acc.measurements_count, acc.sum_us / acc.measurements_count,
				acc.measurements_count
			))
		}
	}

	/**
	  * This class defines when the record with average value should be triggered
	  *     1. When the last record for each window comes. This is to ensure that one output is generated right when an input received,
	  *        no matter how many records have been recorded in that window. This is the FIRST output of that window.
	  *     2. After the last record has come, every single record with event_time earlier than that last record would trigger an UPDATE
	  *
	  * @param slidingInterval
	  */
	class AggTrigger(slidingInterval: Long) extends Trigger[SdcEnrichedInstant, TimeWindow] {
		override def onElement(record: SdcEnrichedInstant, eventTime: Long, w: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
			val triggered: ValueState[Boolean] = ctx.getPartitionedState(
				triggeredStateDescriptor)

			if (triggered.value())
				TriggerResult.FIRE
			else if (eventTime >= w.getEnd - slidingInterval) {
				triggered.update(true)
				TriggerResult.FIRE
			}
			else TriggerResult.CONTINUE
		}

		override def clear(w: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
			ctx.getPartitionedState(triggeredStateDescriptor).clear()
		}

		override def onEventTime(l: Long, w: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

		override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext) = TriggerResult.CONTINUE
	}
}
