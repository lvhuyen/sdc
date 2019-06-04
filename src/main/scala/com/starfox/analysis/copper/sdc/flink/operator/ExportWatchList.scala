package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data.SdcCompact
import com.starfox.analysis.copper.sdc.flink.operator.ExportWatchList.{Key, Toggle}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * This is option 1
  * This KeyedBroadcastProcessFunction has:
  * 	input1: a keyed `DataStream[Either[Toggle, MyEvent]]`:
  * 		input1.left: Toggles in the form of a tuple (Key, Boolean).
  * 			When Toggle._2 == true, records from input1.right for the same key will be forwarded to the main output.
  * 			If it is false, records from input1.right for that same key will be dropped
  * 		input1.right: the main data stream
  *
  * 	input2: a broadcasted stream of StateReport triggers. When a record arrived on this stream,
  * 		the current value of Toggles will be sent out via the outputTag
  */
//class ExportWatchList(outputTag: OutputTag[Toggle])
//		extends KeyedBroadcastProcessFunction[Key, MyEvent, Toggle, MyEvent] {
//
//	val toggleStateDescriptor = new MapStateDescriptor("NoSyncWatchList", classOf[Key], classOf[Boolean])
//
//	override def processElement(in1: MyEvent,
//								readOnlyContext: KeyedBroadcastProcessFunction[Key, MyEvent, Toggle, MyEvent]#ReadOnlyContext,
//								collector: Collector[MyEvent]): Unit = {
//		if (readOnlyContext.getBroadcastState(toggleStateDescriptor).get(readOnlyContext.getCurrentKey))
//			collector.collect(in1)
//	}
//
//	override def processBroadcastElement(in2: Toggle,
//										 context: KeyedBroadcastProcessFunction[Key, MyEvent, Toggle, MyEvent]#Context,
//										 collector: Collector[MyEvent]): Unit = {
//		if (in2._1 == ExportWatchList.EXPORT_MARK)
//			context.applyToKeyedState(toggleStateDescriptor, (k: Key, s: MapState[Key, Boolean]) => {
//				if (s.contains(k))
//					context.output(outputTag, (k, s.get(k)))
//			})
//		else
//			context.getBroadcastState(toggleStateDescriptor).put(in2._1, in2._2)
//	}
//}
//
//object ExportWatchList {
//	type Key = (String, String)
//	type Toggle = (Key, Boolean)
//	type MyEvent = SdcCompact
//
//	val EXPORT_MARK: Key = ("EXPORT", "EXPORT")
//
//	def apply(input: DataStream[MyEvent],
//			  toggle: DataStream[Toggle],
//			  trigger: DataStream[Toggle]): (DataStream[MyEvent], DataStream[Toggle]) = {
//
//		val outputTag = new OutputTag[Toggle]("Dummy")
//		val exporter = new ExportWatchList(outputTag)
//		val toggleStateDescriptor = new MapStateDescriptor("NoSyncWatchList", classOf[Key], classOf[Boolean])
//
//		val tmp: DataStream[MyEvent] = input.keyBy(r => (r.dslam, r. port))
//				.connect((toggle.union(trigger)).broadcast(toggleStateDescriptor)).process(exporter)
//		(tmp, tmp.getSideOutput(outputTag))
//	}
//}

/** end of option 1 */


/** This is option 2 */
/**
  * This KeyedBroadcastProcessFunction has:
  * 	input1: a keyed `DataStream[Either[Toggle, MyEvent]]`:
  * 		input1.left: Toggles in the form of a tuple (Key, Boolean).
  * 			When Toggle._2 == true, records from input1.right for the same key will be forwarded to the main output.
  * 			If it is false, records from input1.right for that same key will be dropped
  * 		input1.right: the main data stream
  *
  * 	input2: a broadcasted stream of StateReport triggers. When a record arrived on this stream,
  * 		the current value of Toggles will be sent out via the outputTag
  */
class ExportWatchList(outputTag: OutputTag[Toggle])
		extends KeyedBroadcastProcessFunction[Key, Either[Toggle, SdcCompact], Boolean, SdcCompact] {

	val toggleStateDescriptor = new ValueStateDescriptor[Boolean]("NoSyncEnabled", classOf[Boolean])

	override def processElement(in1: Either[Toggle, SdcCompact],
								readOnlyContext: KeyedBroadcastProcessFunction[Key, Either[Toggle, SdcCompact], Boolean, SdcCompact]#ReadOnlyContext,
								collector: Collector[SdcCompact]): Unit = {
		in1 match {
			case Left(toggle) =>
				getRuntimeContext.getState(toggleStateDescriptor).update(toggle._2)
			case Right(event) =>
				if (getRuntimeContext.getState(toggleStateDescriptor).value())
					collector.collect(event)
		}
	}

	override def processBroadcastElement(in2: Boolean,
										 context: KeyedBroadcastProcessFunction[Key, Either[Toggle, SdcCompact], Boolean, SdcCompact]#Context,
										 collector: Collector[SdcCompact]): Unit = {
		context.applyToKeyedState(toggleStateDescriptor, (k: Key, s: ValueState[Boolean]) =>
			if (s != null) context.output(outputTag, (k, s.value())))
	}
}

object ExportWatchList {
	type Key = (String, String)
	type Toggle = (Key, Boolean)

	def apply(input: KeyedStream[Either[Toggle, SdcCompact], (String, String)],
			  trigger: DataStream[Boolean]): (DataStream[SdcCompact], DataStream[Toggle]) = {
		val outputTag = new OutputTag[(Key, Boolean)]("Dummy")
		val exporter = new ExportWatchList(outputTag)
		val broadcastStateDescriptor = new MapStateDescriptor(
			"DummyMapState",
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

//		val keyedStream: KeyedStream[Either[((String, String), Boolean), SdcCompact], (String, String)] = input.keyBy(r => r match {
//			case Left(t) => t._1
//			case Right(e) => (e.dslam, e.port)
//		})
//
		val ret: DataStream[SdcCompact] = input.connect(trigger.broadcast(broadcastStateDescriptor)).process(exporter)
		(ret, ret.getSideOutput(outputTag))
	}
}
