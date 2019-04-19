package com.nbnco.csa.analysis.copper.sdc.flink.operator
import com.nbnco.csa.analysis.copper.sdc.flink.source.MergeCsv
import com.nbnco.csa.analysis.copper.sdc.data.DslamRaw
import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Created by Huyen on 19/4/19.
  */
object AggregateRawDslamScala {
	type Accumulator = (Int, String, Map[String, String])

	class AggIncremental extends AggregateFunction[DslamRaw[String], Accumulator, Accumulator] {
		override def createAccumulator() = (0, "", Map.empty[String, String])

		override def add(in: DslamRaw[String], acc: Accumulator): Accumulator = {
			val dslamType = if (in.metadata.isInstant) 1 else 2
			if (acc._1 == 0)
				(dslamType, in.metadata.columns, in.data)
			else
				merge((dslamType, in.metadata.columns, in.data), acc)
		}

		override def merge(acc: Accumulator, acc2: Accumulator): Accumulator = {
			if ((acc._1 & acc2._1) > 0 || acc._1 == 0 || acc2._1 == 0)
				throw InvalidDataException("Duplicated data")
			else {
				val (c, r) = MergeCsv(acc._2, acc._3, acc2._2, acc2._3)
				(acc._1 | acc2._1, c, r)
			}
		}

		override def getResult(acc: Accumulator): Accumulator = acc
	}

	class AggFinal extends ProcessWindowFunction [Accumulator, (Long, String, String, Map[String, String]), String, TimeWindow] {
		override def process(key: String, context: Context, elements: Iterable[Accumulator], out: Collector[(Long, String, String, Map[String, String])]): Unit = {
			val (_, a, b) = elements.iterator.next()
			out.collect((context.window.getStart, key, a, b))
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
	class AggTrigger() extends Trigger[Accumulator, TimeWindow] {

		override def onElement(element: Accumulator, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
			if (element._1 == 3) TriggerResult.FIRE_AND_PURGE
			else TriggerResult.CONTINUE
		}

		override def onEventTime(l: Long, w: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.FIRE_AND_PURGE

		override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
			TriggerResult.CONTINUE
		}

		override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
		}
	}
}
