//package com.nbnco.csa.analysis.copper.sdc.flink.operator
//
//import com.nbnco.csa.analysis.copper.sdc.data.DslamType._
//import com.nbnco.csa.analysis.copper.sdc.data.{DslamFile, DslamMetadata, DslamRaw, mergeCsv}
//import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.state.ValueStateDescriptor
//import org.apache.flink.core.fs.FileInputSplit
//import org.apache.flink.streaming.api.scala.createTypeInformation
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//
///**
//  * Created by Huyen on 19/4/19.
//  */
//object AggregateRawDslamByFileName {
//	type Accumulator = (Int, String, List[FileInputSplit])
//	val ACC_EMPTY: Accumulator = (0, "", null)
//
//	private val stateDescSubsequentItem = new ValueStateDescriptor[Int]("SubsequentItem", createTypeInformation[Int])
//
//	class AggIncremental extends AggregateFunction[DslamFile, Accumulator, Accumulator] {
//		override def createAccumulator(): Accumulator = ACC_EMPTY
//
//		override def add(in: DslamRaw[String], acc: Accumulator): Accumulator = {
//			val dslamType = in.dslamType
//			if (dslamType == acc._1)
//				acc
//			else if (acc.eq(ACC_EMPTY))
//				(dslamType, in.metadata.columns, in.data)
//			else {
//				val (c, r) = mergeCsv(acc._2, acc._3, in.metadata.columns, in.data)
//				(acc._1 | dslamType, c, r)
//			}
//		}
//
//		override def merge(acc: Accumulator, acc2: Accumulator): Accumulator = {
//			throw InvalidDataException("Merge operation is not expected")
//		}
//
//		override def getResult(acc: Accumulator): Accumulator = acc
//	}
//
//	class AggFinal extends ProcessWindowFunction [Accumulator, DslamRaw[String], String, TimeWindow] {
//		override def process(key: String, context: Context, elements: Iterable[Accumulator], out: Collector[DslamRaw[String]]): Unit = {
//			val (_, columns, data) = elements.iterator.next()
//			out.collect(DslamRaw(context.window.getStart, key, data, DslamMetadata(columns)))
//		}
//	}
//
//	/**
//	  * This class defines when the output should be sent out. As we expect only 2 input records (H & I) per window
//	  *     1. When the first record comes, it is stored in cache (Accumulator), no output is sent
//	  *     2. When multiple records of the same type (H or I) arrive, then the 2nd record is ignored.
//	  *     	Warning should be raised.
//	  *     3. When both H & I records have arrived, then the merged output it sent out.
//	  *     	Cache is cleared
//	  *
//	  * @param slidingInterval
//	  */
//	class AggTrigger() extends Trigger[DslamRaw[String], TimeWindow] {
//
//		override def onElement(element: DslamRaw[String], timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
//			val cachedState = ctx.getPartitionedState(stateDescSubsequentItem)
//
//			val incomingRecordType: Int = if (element.metadata.isInstant) 1 else 2
//
//			Option(cachedState.value()).getOrElse(DSLAM_NONE) match {
//					case DSLAM_COMBINED =>
//						TriggerResult.PURGE
//					case DSLAM_NONE =>
//						cachedState.update(incomingRecordType)
//						TriggerResult.CONTINUE
//					case cachedType =>
//						if (cachedType != incomingRecordType) {
//							cachedState.update(DSLAM_COMBINED)
//							TriggerResult.FIRE_AND_PURGE
//						} else
//							TriggerResult.CONTINUE
//			}
//		}
//
//		override def onEventTime(l: Long, w: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.FIRE
//
//		override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
//			TriggerResult.CONTINUE
//
//		override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
//			val state = triggerContext.getPartitionedState(stateDescSubsequentItem)
//			state.clear()
//			val a = 5
//		}
//	}
//}
