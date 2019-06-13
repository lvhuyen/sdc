package com.starfox.analysis.copper.sdc.flink.operator

import com.starfox.analysis.copper.sdc.data.DslamType._
import com.starfox.analysis.copper.sdc.data._
import com.starfox.analysis.copper.sdc.flink.source.{SdcFilePathFilter, SdcTarInputFormat, SdcTarInputFormatLite}
import com.starfox.flink.source.SmallFilesReader
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Created by Huyen on 19/4/19.
  */
object ReadAndCombineRawSdcFiles {
	/**
	  * This function reads then combine the two I & H files.
	  * @param streamEnv
	  * @param cfgSdcInstantLocation
	  * @param cfgSdcHistoricalLocation
	  * @param cfgSdcScanInterval
	  * @param cfgSdcScanConsistency
	  * @param cfgSdcIgnoreThreshold
	  * @param allowedLateness
	  * @param tumblingWindowSize
	  * @return: a tuple of three DataStreams:
	  *         One raw stream of type DslamRaw[ Map[String,String] ] which contains the merged of I&H data
	  *         One stream of un-matched dslam files (either I or H)
	  *         One stream metadata, one record for each I or H file.
	  */
	def apply(streamEnv: StreamExecutionEnvironment,
			  cfgSdcInstantLocation: String, cfgSdcHistoricalLocation: String,
			  cfgSdcScanInterval: Long, cfgSdcScanConsistency: Long, cfgSdcIgnoreThreshold: Long,
			  allowedLateness: Long, tumblingWindowSize: Long = 900000L,
			  readAfterCombine: Boolean = false):
	(DataStream[DslamRaw[Map[String, String]]], DataStream[DslamRaw[None.type]], DataStream[DslamRaw[None.type]]) = {

		if (readAfterCombine)
			readSdcData2(streamEnv, cfgSdcInstantLocation, cfgSdcHistoricalLocation,cfgSdcScanInterval, cfgSdcScanConsistency, cfgSdcIgnoreThreshold, allowedLateness, tumblingWindowSize)
		else
			readSdcData(streamEnv, cfgSdcInstantLocation, cfgSdcHistoricalLocation,cfgSdcScanInterval, cfgSdcScanConsistency, cfgSdcIgnoreThreshold, allowedLateness, tumblingWindowSize)
	}

	/**
	  * This function reads the files, then merges later
	  */
	private def readSdcData(streamEnv: StreamExecutionEnvironment,
					 cfgSdcInstantLocation: String, cfgSdcHistoricalLocation: String,
					 cfgSdcScanInterval: Long, cfgSdcScanConsistency: Long, cfgSdcIgnoreThreshold: Long,
					 allowedLateness: Long, tumplingWindowSize: Long = 900000L):
	(DataStream[DslamRaw[Map[String, String]]], DataStream[DslamRaw[None.type]], DataStream[DslamRaw[None.type]]) = {

		val pathSdcHistorical = new org.apache.flink.core.fs.Path(cfgSdcHistoricalLocation)
		val fifSdcHistorical = new SdcTarInputFormat(pathSdcHistorical, "Historical")
		fifSdcHistorical.setFilesFilter(new SdcFilePathFilter(cfgSdcIgnoreThreshold))
		fifSdcHistorical.setNestedFileEnumeration(true)

		val pathSdcInstant = new org.apache.flink.core.fs.Path(cfgSdcInstantLocation)
		val fifSdcInstant = new SdcTarInputFormat(pathSdcInstant, "Instant")
		fifSdcInstant.setFilesFilter(new SdcFilePathFilter(cfgSdcIgnoreThreshold))
		fifSdcInstant.setNestedFileEnumeration(true)

		type IntermediateDataType = DslamRaw[Map[String, String]]

		val streamDslamH: DataStream[IntermediateDataType] = SmallFilesReader
				.readFile(streamEnv,
					fifSdcHistorical,
					cfgSdcHistoricalLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcScanInterval, cfgSdcScanConsistency)
				.uid(OperatorId.SOURCE_SDC_HISTORICAL + "_v0")
				.name("Read SDC Historical")

		val streamDslamI: DataStream[IntermediateDataType] = SmallFilesReader
				.readFile(streamEnv,
					fifSdcInstant,
					cfgSdcInstantLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcScanInterval, cfgSdcScanConsistency)
				.uid(OperatorId.SOURCE_SDC_INSTANT + "_v0")
				.name("Read SDC Instant")

		val streamDslamFull: DataStream[IntermediateDataType] = streamDslamH.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[IntermediateDataType])
				.union(streamDslamI.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[IntermediateDataType]))

		val outputTagUnmatched = OutputTag[IntermediateDataType]("unmatched dslam record")
		val streamDslamMerged = streamDslamFull.keyBy(_.name)
				.window(TumblingEventTimeWindows.of(Time.milliseconds(tumplingWindowSize)))
				.trigger(new ReadAndCombineRawSdcFiles.AggTrigger)
				.allowedLateness(Time.milliseconds(allowedLateness))
				.sideOutputLateData(outputTagUnmatched)
				.process(new ReadAndCombineRawSdcFiles.ByPortAggregator(outputTagUnmatched))
        		.uid(OperatorId.SDC_COMBINER + "_v0")

		(streamDslamMerged,
				streamDslamMerged.getSideOutput(outputTagUnmatched).map(r => r.copy(data = None)),
				streamDslamI.union(streamDslamH).map(r => r.copy(data = None))
		)
	}

	/**
	  * This functions waits for both files to arrive, then read and merge them
	  */
	private def readSdcData2(streamEnv: StreamExecutionEnvironment,
					cfgSdcInstantLocation: String, cfgSdcHistoricalLocation: String,
					cfgSdcScanInterval: Long, cfgSdcScanConsistency: Long, cfgSdcIgnoreThreshold: Long,
					allowedLateness: Long, tumplingWindowSize: Long = 900000L):
	(DataStream[DslamRaw[Map[String, String]]], DataStream[DslamRaw[None.type]], DataStream[DslamRaw[None.type]]) = {

		val pathSdcHistorical = new org.apache.flink.core.fs.Path(cfgSdcHistoricalLocation)
		val fifSdcHistorical = new SdcTarInputFormatLite(pathSdcHistorical, "Historical")
		fifSdcHistorical.setFilesFilter(new SdcFilePathFilter(cfgSdcIgnoreThreshold))
		fifSdcHistorical.setNestedFileEnumeration(true)

		val pathSdcInstant = new org.apache.flink.core.fs.Path(cfgSdcInstantLocation)
		val fifSdcInstant = new SdcTarInputFormatLite(pathSdcInstant, "Instant")
		fifSdcInstant.setFilesFilter(new SdcFilePathFilter(cfgSdcIgnoreThreshold))
		fifSdcInstant.setNestedFileEnumeration(true)

		type IntermediateDataType = DslamRaw[FileInputSplit]

		val streamDslamH: DataStream[IntermediateDataType] = SmallFilesReader
				.readFile(streamEnv, fifSdcHistorical,
					cfgSdcHistoricalLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcScanInterval,
					cfgSdcScanConsistency)
				.uid(OperatorId.SOURCE_SDC_HISTORICAL)
				.name("Read SDC Historical")

		val streamDslamI: DataStream[IntermediateDataType] = SmallFilesReader
				.readFile(streamEnv, fifSdcInstant,
					cfgSdcInstantLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcScanInterval,
					cfgSdcScanConsistency)
				.uid(OperatorId.SOURCE_SDC_INSTANT)
				.name("Read SDC Instant")

		val streamDslamFull: DataStream[IntermediateDataType] = streamDslamH.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[IntermediateDataType])
				.union(streamDslamI.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[IntermediateDataType]))

		val outputTagUnmatched = OutputTag[IntermediateDataType]("unmatched dslam record")
		val outputTagMetadata = OutputTag[DslamRaw[None.type]]("dslam metadata record")
		val streamDslamMerged = streamDslamFull.keyBy(_.name)
				.window(TumblingEventTimeWindows.of(Time.milliseconds(tumplingWindowSize)))
				.trigger(new AggTrigger)
				.allowedLateness(Time.milliseconds(allowedLateness))
				.sideOutputLateData(outputTagUnmatched)
				.process(new ByFileAggregator(pathSdcInstant, outputTagUnmatched, outputTagMetadata))
				.uid(OperatorId.SDC_COMBINER)

		(streamDslamMerged,
				streamDslamMerged.getSideOutput(outputTagUnmatched).map{
					r => r.copy(data = None, metadata = DslamMetadata(r.data.getPath.getPath))
				},
				streamDslamMerged.getSideOutput(outputTagMetadata)
		)
	}

	def combineUsingTableAPI() = {
		/** Use Table API to merge two streams I and H
		import scala.collection.JavaConverters._
		val streamDslamFull = streamDslamInstant.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
								.union(streamDslamHistorical.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner2))
        		.map(r => (r.metadata.ts, r.metadata.isInstant, r.metadata.name, r.metadata.columns, r.data.asJava))
		val tableEnv = StreamTableEnvironment.create(streamEnv)
		val tableDslamFull = tableEnv.fromDataStream(streamDslamFull, 'ts.rowtime, 'isInstant, 'name, 'columns, 'data)
		tableEnv.registerFunction("AggDslam", new AggregateRawDslamJavaTableAPI())
		val windowedTable = tableDslamFull
				.window(Tumble over 900.seconds on 'ts as 'sdcWindow)
				.groupBy('sdcWindow, 'name)
				.select("sdcWindow.start, name, AggDslam(isInstant, columns, data)")
		val streamResult = tableEnv.toRetractStream[(java.sql.Timestamp, String, DslamCompact)](windowedTable)
		streamResult.startNewChain().print() */
	}


	private val stateDescPreviousDslamType = new ValueStateDescriptor[Int]("PreviousDslamType", createTypeInformation[Int])

	/**
	  * This class defines when the output should be sent out. As we expect only 2 input records (H & I) per window
	  *     1. When the first record comes, it is stored in cache (Accumulator), no output is sent
	  *     2. When multiple records of the same type (H or I) arrive, then the 2nd record is ignored.
	  *     	Warning should be raised.
	  *     3. When both H & I records have arrived, then the merged output it sent out.
	  *     	Cache is cleared
	  */
	class AggTrigger[DataType]() extends Trigger[DslamRaw[DataType], TimeWindow] {

		override def onElement(element: DslamRaw[DataType], timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
			val cachedState = ctx.getPartitionedState(stateDescPreviousDslamType)

			Option(cachedState.value()).getOrElse(DSLAM_NONE) match {
				case DSLAM_COMBINED =>
					TriggerResult.PURGE
				case DSLAM_NONE =>
					cachedState.update(element.dslamType)
					TriggerResult.CONTINUE
				case cachedType =>
					if (cachedType != element.dslamType) {
						cachedState.update(DSLAM_COMBINED)
						TriggerResult.FIRE_AND_PURGE
					} else
						TriggerResult.CONTINUE
			}
		}

		override def onEventTime(l: Long, w: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.FIRE

		override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
			TriggerResult.CONTINUE

		override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
			triggerContext.getPartitionedState(stateDescPreviousDslamType).clear()
		}
	}

	/***
	  * This is the Window function that merge two DslamRaw[ Map[String, String] ]. The output is also a DslamRaw[Map[String, String] ]
	  */

	private class ByPortAggregator(unmatchedSideOutput: OutputTag[DslamRaw[Map[String, String]]])
			extends ProcessWindowFunction [DslamRaw[Map[String, String]], DslamRaw[Map[String, String]], String, TimeWindow] {

		override def process(key: String, context: Context, elements: Iterable[DslamRaw[Map[String, String]]], out: Collector[DslamRaw[Map[String, String]]]): Unit = {
			(elements.find(_.dslamType == DslamType.DSLAM_INSTANT), elements.find(_.dslamType == DslamType.DSLAM_HISTORICAL)) match {
				case (Some(i), Some(h)) =>
					val data = mergeCsv(i.metadata.columns, i.data, h.metadata.columns, h.data)
							out.collect(DslamRaw(i.ts, i.name, DslamType.DSLAM_COMBINED, data,
								DslamMetadata(i.metadata, h.metadata, data.size)))
				case _ =>
					logUnmatchedRecord(elements, context)
			}
		}
		private def logUnmatchedRecord(elements: Iterable[DslamRaw[Map[String, String]]], context: Context): Unit = {
			elements.foreach(context.output(unmatchedSideOutput, _))
		}
	}


	private class ByFileAggregator(sampleFilePath: Path,
								   unmatchedSideOutput: OutputTag[DslamRaw[FileInputSplit]],
								   metadataSideOutput: OutputTag[DslamRaw[None.type]])
			extends ProcessWindowFunction [DslamRaw[FileInputSplit], DslamRaw[Map[String, String]], String, TimeWindow] {

		private lazy val reader = new SdcTarInputFormat(sampleFilePath, "")
		private val EMPTY_DSLAM: DslamRaw[Map[String, String]] = DslamRaw(0, "", DslamType.DSLAM_NONE, Map.empty, DslamMetadata.EMPTY)

		override def open(parameters: Configuration): Unit = {
			super.open(parameters)
			reader.setRuntimeContext(this.getRuntimeContext)
		}

		override def process(key: String, context: Context, elements: Iterable[DslamRaw[FileInputSplit]], out: Collector[DslamRaw[Map[String, String]]]): Unit = {
			(parseToPort(elements.find(_.dslamType == DslamType.DSLAM_INSTANT)),
					parseToPort(elements.find(_.dslamType == DslamType.DSLAM_HISTORICAL))) match {

				case (Some(i), Some(h)) =>
					context.output(metadataSideOutput, i.copy(data = None))
					context.output(metadataSideOutput, h.copy(data = None))

					val data = mergeCsv(i.metadata.columns, i.data, h.metadata.columns, h.data)
					out.collect(DslamRaw(i.ts, i.name, DslamType.DSLAM_COMBINED, data,
						DslamMetadata(i.metadata, h.metadata, data.size)))
				case _ =>
					logUnmatchedRecord(elements, context)
			}
		}

		private def parseToPort(in: Option[DslamRaw[FileInputSplit]]) = {
			in match {
				case Some(file) =>
					reader.open(file.data)
					val out = reader.nextRecord(EMPTY_DSLAM)
					reader.close()
					Option(out)
				case _ =>
					None
			}
		}

		private def logUnmatchedRecord(elements: Iterable[DslamRaw[FileInputSplit]], context: Context): Unit = {
			elements.foreach(context.output(unmatchedSideOutput, _))
		}
	}
}
