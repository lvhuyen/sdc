package com.nbnco.csa.analysis.copper.sdc.flink.source

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.{ContinuousFileReaderOperator, FileProcessingMode}
import org.apache.flink.streaming.api.scala._


object SdcFiles {
	def readFile[OUT: TypeInformation](streamEnv: StreamExecutionEnvironment,
									   inputFormat: FileInputFormat[OUT],
									   filePath: String,
									   watchType: FileProcessingMode,
									   interval: Long,
									   readConsistencyOffset: Long): DataStream[OUT] = {
		val monitoringFunction = new ContinuousFileMonitoringFunction[OUT](inputFormat, watchType, streamEnv.getParallelism, interval, readConsistencyOffset)

		val reader = new ContinuousFileReaderOperator[OUT](inputFormat)

		val source = streamEnv.addSource(monitoringFunction).setParallelism(1).transform("Split Reader: " + filePath, reader)
		source
	}
}
