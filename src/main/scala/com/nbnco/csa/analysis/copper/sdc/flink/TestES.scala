package com.nbnco.csa.analysis.copper.sdc.flink

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.nbnco.csa.analysis.copper.sdc.data._
import com.nbnco.csa.analysis.copper.sdc.flink.operator._
import com.nbnco.csa.analysis.copper.sdc.flink.sink.{SdcElasticSearchSink, SdcParquetFileSink}
import com.nbnco.csa.analysis.copper.sdc.flink.source._
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object TestES {
	object OperatorId {
		val SOURCE_SDC_INSTANT = "Source_Sdc_Instant"
		val SOURCE_SDC_HISTORICAL = "Source_Sdc_Historical"
		val SOURCE_FLS_INITIAL = "Source_Fls_Initial"
		val SOURCE_FLS_INCREMENTAL = "Source_Fls_Incremental"
		val SOURCE_FLS_RAW = "Source_Fls_Raw"
		val SOURCE_AMS_RAW = "Source_Ams_Raw"
		val SOURCE_NAC_RAW = "Source_Nac_Raw"
		val SOURCE_ATTEN375_RAW = "Source_Atten375_Raw"
		val SOURCE_PERCENTILES_RAW = "Source_Percentiles_Raw"
		val SINK_ELASTIC_ENRICHMENT = "Elastic_Enrichment"
		val SINK_ELASTIC_COMBINED = "Elastic_Combined"
		val SINK_ELASTIC_AVERAGE = "Elastic_Average"
		val SINK_ELASTIC_UNENRICHABLE = "Elastic_Unenrichable"
		val SINK_ELASTIC_METADATA = "Elastic_Dslam_Metadata"
		val SINK_ELASTIC_MISSING_I = "Elastic_Dslam_Missing_Instant"
		val SINK_ELASTIC_MISSING_H = "Elastic_Dslam_Missing_Historical"
		val SINK_PARQUET_INSTANT = "S3_Parquet_Instant"
		val SINK_PARQUET_HISTORICAL = "S3_Parquet_Historical"
		val SINK_PARQUET_METADATA = "S3_Parquet_Metadata"
		val SINK_PARQUET_UNENRICHABLE_I = "S3_Parquet_Unenrichable_Instant"
		val SINK_PARQUET_UNENRICHABLE_H = "S3_Parquet_Unenrichable_Historical"
		val SDC_ENRICHER = "Sdc_Enricher"
		val SDC_AVERAGER = "Sdc_Averager"
		val SDC_COMBINER = "Sdc_Combiner"
		val STATS_COUNT_HISTORICAL = "Stats_Count_Historical"
		val STATS_COUNT_INSTANT = "Stats_Count_Instant"
		val STATS_MISSING_DSLAM_HISTORICAL = "Stats_Missing_Dslam_Historical"
		val STATS_MISSING_DSLAM_INSTANT = "Stats_Missing_Dslam_Instant"
		val AMS_FLS_MERGER = "Ams_Fls_Merger"
		val SDC_PARSER_HISTORICAL = "Sdc_Parser_Historical"
		val SDC_PARSER_INSTANT = "Sdc_Parser_Instant"
	}

	def initEnvironment(appConfig: ParameterTool): StreamExecutionEnvironment = {
		val cfgParallelism = appConfig.getInt("parallelism", 32)
		val cfgTaskTimeoutInterval = appConfig.getLong("task-timeout-interval", 30000)
		val cfgCheckpointEnabled = appConfig.getBoolean("checkpoint.enabled", true)
		val cfgCheckpointLocation = appConfig.get("checkpoint.path", "s3://assn-csa-prod-telemetry-data-lake/TestData/Spike/Huyen/SDC/ckpoint/")
		val cfgCheckpointInterval = appConfig.getLong("checkpoint.interval", 800000L)
		val cfgCheckpointTimeout = appConfig.getLong("checkpoint.timeout", 600000L)
		val cfgCheckpointMinPause = appConfig.getLong("checkpoint.minimum-pause", 600000L)
		val cfgCheckpointStopJobWhenFail = appConfig.getBoolean("checkpoint.stop-job-when-fail", true)

//		val streamEnv =
//			if (cfgCheckpointEnabled) StreamExecutionEnvironment.getExecutionEnvironment
//					.setStateBackend(new RocksDBStateBackend(cfgCheckpointLocation, true))
//					.enableCheckpointing(cfgCheckpointInterval)
//			else StreamExecutionEnvironment.getExecutionEnvironment


		val streamEnv =
			if (false) StreamExecutionEnvironment.getExecutionEnvironment
					.setStateBackend(new FsStateBackend(cfgCheckpointLocation, true))
					.enableCheckpointing(cfgCheckpointInterval)
			else StreamExecutionEnvironment.getExecutionEnvironment


		if (cfgCheckpointEnabled) {
			streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(cfgCheckpointMinPause)
			streamEnv.getCheckpointConfig.setCheckpointTimeout(cfgCheckpointTimeout)
			streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(cfgCheckpointStopJobWhenFail)
			streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
			streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
		}
		streamEnv.getConfig.setTaskCancellationInterval(cfgTaskTimeoutInterval)
		streamEnv.setParallelism(cfgParallelism)
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		return streamEnv
	}

	def main(args: Array[String]) {

		/** Read configuration */
		val configFile = ParameterTool.fromArgs(args).get("configFile", "dev.properties")

		import java.net.URI
		val fs = org.apache.flink.core.fs.FileSystem.get(URI.create(configFile))
		val appConfig = ParameterTool.fromPropertiesFile(fs.open(new Path(configFile)))
		val debug = appConfig.getBoolean("debug", false)

		val cfgElasticSearchEndpoint = appConfig.get("sink.elasticsearch.endpoint", "vpc-assn-dev-chronos-devops-pmwehr2e7xujnadk53jsx4bjne.ap-southeast-2.es.amazonaws.com")
		val cfgElasticSearchBulkActions = appConfig.getInt("sink.elasticsearch.bulk-actions", 10000)
		val cfgElasticSearchBulkSize = appConfig.getInt("sink.elasticsearch.bulk-size-mb", 10)
		val cfgElasticSearchMaxRetries = appConfig.getInt("sink.elasticsearch.max-retries-in-nine-minutes", 3 * cfgElasticSearchBulkActions)
		val cfgElasticSearchBulkInterval = appConfig.getInt("sink.elasticsearch.bulk-interval-ms", 60000)
		val cfgElasticSearchBackoffDelay = appConfig.getInt("sink.elasticsearch.bulk-flush-backoff-delay-ms", 100)

		val cfgElasticSearchCombinedSdcEnabled = appConfig.getBoolean("sink.elasticsearch.sdc.enabled", false)
		val cfgElasticSearchCombinedSdcIndexName = appConfig.get("sink.elasticsearch.sdc.index-name", "copper-sdc-combined-default")
		val cfgElasticSearchCombinedSdcParallelism = appConfig.getInt("sink.elasticsearch.sdc.parallelism", 4)

		val cfgElasticSearchAverageParallelism = appConfig.getInt("sink.elasticsearch.average.parallelism", 4)

		val cfgElasticSearchEnrichmentEnabled = appConfig.getBoolean("sink.elasticsearch.enrichment.enabled", false)
		val cfgElasticSearchEnrichmentIndexName = appConfig.get("sink.elasticsearch.enrichment.index-name", "copper-enrichment-default")
		val cfgElasticSearchEnrichmentParallelism = appConfig.getInt("sink.elasticsearch.enrichment.parallelism", 1)

		val cfgElasticSearchStatsEnabled = appConfig.getBoolean("sink.elasticsearch.stats.enabled", false)
		val cfgElasticSearchStatsDslamMetaIndexName = appConfig.get("sink.elasticsearch.stats.dslam-meta.index-name", "copper-sdc-dslam-metadata")
		val cfgElasticSearchStatsMissingInstantIndexName = appConfig.get("sink.elasticsearch.stats.missing-instant.index-name", "copper-sdc-dslam-missing-instant")
		val cfgElasticSearchStatsMissingHistoricalIndexName = appConfig.get("sink.elasticsearch.stats.missing-historical.index-name", "copper-sdc-dslam-missing-historical")

		val cfgRollingAverageWindowInterval = appConfig.getLong("rolling-average.window-interval-minutes", 120)
		val cfgRollingAverageSlideInterval = appConfig.getLong("rolling-average.slide-interval-minutes", 15)
		val cfgRollingAverageAllowedLateness = appConfig.getLong("rolling-average.allowed-lateness-minutes", 600)

		val cfgParquetEnabled = appConfig.getBoolean("sink.parquet.enabled", false)
		val cfgParquetParallelism = appConfig.getInt("sink.parquet.parallelism", 4)
		val cfgParquetPrefix = appConfig.get("sink.parquet.prefix", "")
		val cfgParquetSuffixFormat = appConfig.get("sink.parquet.suffixFormat", "yyyy-MM-dd-hh")
		val cfgParquetInstantPath = appConfig.get("sink.parquet.instant.path", "file:///Users/Huyen/Desktop/SDCTest/output/instant")
		val cfgParquetHistoricalPath = appConfig.get("sink.parquet.historical.path", "file:///Users/Huyen/Desktop/SDCTest/output/historical")
		val cfgParquetMetadataPath = appConfig.get("sink.parquet.metadata", "file:///Users/Huyen/Desktop/SDCTest/output/metadata")

		/** set up the streaming execution environment */
		val streamEnv = initEnvironment(appConfig)


		/** Read input text file */
			val path = "file:///Users/Huyen/Desktop/SDCTest/input/es"
		val streamInput = streamEnv.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
        		.filter(_.startsWith("AVC"))


		val serverUrl = "vpc-csa-chronos-analytics-prod-r3g6gdlvwljkufo3i5mv4mbw74.ap-souatheast-2.es.amazonaws.com"
		val indexName = "sdc-counters-prod*"
		val docType = "counters"
		val cnt = 2
		val timeout = 3

		val streamEsSearchResponse = AsyncDataStream.unorderedWait(streamInput.map((_, true)),
			new ReadHistoricalDataFromES(serverUrl, indexName, docType, cnt, timeout), timeout + 10, TimeUnit.SECONDS, 100)

		val tagToggle = OutputTag[((String, String), Boolean)]("NoSyncTogglePhysicalRef")
		val tagRetry = OutputTag[(String, Boolean)]("NoSyncToggleRetry")

		val streamEsData = streamEsSearchResponse.process(new ParseEsQueryResult(tagToggle, tagRetry))

		streamInput.print()
		streamEsData.print()
		streamEsData.getSideOutput(tagToggle).print()
		streamEsData.getSideOutput(tagRetry).print()

		// execute program
		streamEnv.execute("Chronos SDC")
	}
}