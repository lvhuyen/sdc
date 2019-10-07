package com.starfox.flink.source

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.{ContinuousFileReaderOperator, FileProcessingMode}
import org.apache.flink.streaming.api.scala._


object SmallFilesReader {
	/**
	 * Reads the contents of the user-specified path based on the given [[FileInputFormat]].
	 * Depending on the provided [[FileProcessingMode]], the source
	 * may periodically monitor (every `interval` ms) the path for new data
	 * ([[FileProcessingMode.PROCESS_CONTINUOUSLY]]), or process
	 * once the data currently in the path and exit
	 * ([[FileProcessingMode.PROCESS_ONCE]]). In addition,
	 * if the path contains files not to be processed, the user can specify a custom
	 * [[FilePathFilter]]. As a default implementation you can use
	 * [[FilePathFilter.createDefaultFilter()]].
	 *
	 * ** NOTES ON CHECKPOINTING: ** If the `watchType` is set to
	 * [[FileProcessingMode#PROCESS_ONCE]], the source monitors the path ** once **,
	 * creates the [[org.apache.flink.core.fs.FileInputSplit FileInputSplits]]
	 * to be processed, forwards them to the downstream
	 * [[ContinuousFileReaderOperator readers]] to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This
	 * implies that no more checkpoint barriers are going to be forwarded
	 * after the source exits, thus having no checkpoints after that point.
	 *
	 * @param inputFormat
	 *          The input format used to create the data stream
	 * @param filePath
	 *          The path of the file, as a URI (e.g., "file:///some/local/file" or
	 *          "hdfs://host:port/file/path")
	 * @param watchType
	 *          The mode in which the source should operate, i.e. monitor path and react
	 *          to new data, or process once and exit
	 * @param interval
	 *          In the case of periodic path monitoring, this specifies the interval (in millis)
	 *          between consecutive path scans
	 * @param readConsistencyOffset
	 *          The buffer period (in millis) to lookback for late coming files.
	 * @return The data stream that represents the data read from the given file
	 */
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