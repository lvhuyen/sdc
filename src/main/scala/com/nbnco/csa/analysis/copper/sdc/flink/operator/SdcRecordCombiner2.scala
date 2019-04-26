//package com.nbnco.csa.analysis.copper.sdc.flink.operator
//
//import com.nbnco.csa.analysis.copper.sdc.data.{SdcCombined, SdcEnrichedBase, SdcEnrichedHistorical, SdcEnrichedInstant}
//import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.util.Collector
///**
//  * Created by Huyen on 15/8/18.
//  */
///**
//  * This ProcessFunction is used to combine Historical with Instant SDC records
//  */
//class SdcRecordCombiner2(val instantDataWait: Long, val historicalDataWait: Long, cacheTTL: Long) extends CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined] {
//	val TIMER_COALESCE_INTERVAL = 5000
//	if ((instantDataWait != 0 && instantDataWait > cacheTTL - TIMER_COALESCE_INTERVAL)
//			|| (historicalDataWait != 0 && historicalDataWait > cacheTTL - TIMER_COALESCE_INTERVAL)) {
//		throw InvalidDataException(s"Incorrect configuration. Time wait for either Historical or Instant data should be smaller than TTL - $TIMER_COALESCE_INTERVAL")
//	}
//
//	lazy val cache: MapState[Long, (SdcEnrichedBase, Boolean)] =
//		getRuntimeContext.getMapState(new MapStateDescriptor[Long, (SdcEnrichedBase, Boolean)]("Cache", classOf[Long], classOf[(SdcEnrichedBase, Boolean)]))
//
//	lazy val timer: MapState[Long, Set[Long]] = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Set[Long]]("Timer", classOf[Long], classOf[Set[Long]]))
//	private def pushToCache(in1: SdcEnrichedBase, waitTime: Long, context: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#Context) = {
//		cache.put(in1.ts, (in1, false))
//		val future1 = ((System.currentTimeMillis + waitTime) / TIMER_COALESCE_INTERVAL) * TIMER_COALESCE_INTERVAL
//		val future2 = ((System.currentTimeMillis + cacheTTL) / TIMER_COALESCE_INTERVAL) * TIMER_COALESCE_INTERVAL
//		val list1 = timer.get(future1)
//		val list2 = timer.get(future2)
//		timer.put(future1, (if (list1 == null) Set() else list1) ++ Set(in1.ts))
//		timer.put(future2, (if (list2 == null) Set() else list2) ++ Set(in1.ts))
//		context.timerService.registerProcessingTimeTimer(future1)
//		context.timerService.registerProcessingTimeTimer(future2)
//	}
//	override def processElement1(in1: SdcEnrichedInstant, context: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#Context, collector: Collector[SdcCombined]): Unit = {
//		cache.get(in1.ts) match {
//			case (h: SdcEnrichedHistorical, _) =>
//				collector.collect(SdcCombined(in1, h))
//				cache.remove(in1.ts)
//			case null =>
//				pushToCache(in1, instantDataWait, context)
//			case _ =>
//		}
//	}
//	override def processElement2(in2: SdcEnrichedHistorical, context: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#Context, collector: Collector[SdcCombined]): Unit = {
//		cache.get(in2.ts) match {
//			case (i: SdcEnrichedInstant, _) =>
//				collector.collect(SdcCombined(i, in2))
//				cache.remove(in2.ts)
//			case null =>
//				pushToCache(in2, historicalDataWait, context)
//			case _ =>
//		}
//	}
//	override def onTimer(timestamp: Long, ctx: CoProcessFunction[SdcEnrichedInstant, SdcEnrichedHistorical, SdcCombined]#OnTimerContext, collector: Collector[SdcCombined]): Unit = {
//		super.onTimer(timestamp, ctx, collector)
//		val list = timer.get(timestamp)
//		list.foreach(ts => {
//			cache.get(ts) match {
//				case (_, true) => cache.remove(ts)
//				case (item: SdcEnrichedBase, false) =>
//					collector.collect(SdcCombined(item))
//					cache.put(ts, (item, true))
//				case _ =>
//			}
//		})
//		timer.remove(timestamp)
//	}
//	//	override def open(parameters: Configuration): Unit =  {
//	//		super.open(parameters)
//	//
//	//		val ttlConfig = StateTtlConfig
//	//				.newBuilder(Time.milliseconds(cacheTTL))
//	//				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//	//				.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
//	//				.cleanupFullSnapshot()
//	//				.build
//	//
//	//		val cacheDescriptor = new ValueStateDescriptor[SdcEnriched]("SDC_Combine", classOf[SdcEnriched])
//	//		cacheDescriptor.enableTimeToLive(ttlConfig)
//	//
//	//		cache = getRuntimeContext.getState(cacheDescriptor)
//	//	}
//}