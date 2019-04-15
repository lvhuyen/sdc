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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.elasticsearch.action.search.SearchResponse

object StreamingSdc {
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
			if (cfgCheckpointEnabled) StreamExecutionEnvironment.getExecutionEnvironment
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

	def readEnrichmentData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[EnrichmentRecord] = {
		val cfgEnrichmentScanInterval = appConfig.getLong("sources.enrichment.scan-interval", 60000L)
		val cfgEnrichmentScanConsistency = appConfig.getLong("sources.fls-parquet.scan-consistency-offset", 0L)
		val cfgEnrichmentIgnoreOlderThan = appConfig.getInt("sources.enrichment.ignore-files-older-than-days", 1)
		val cfgEnrichmentReaderParallelism =
			Math.min(streamEnv.getParallelism, appConfig.getInt("sources.enrichment.reader-parallelism", 8))

		val cfgFlsParquetLocation = appConfig.get("sources.fls-parquet.path", "s3://thor-pr-data-warehouse-common/common.ipact.fls.fls7_avc_inv/version=0/")
		val cfgAmsParquetLocation = appConfig.get("sources.ams-parquet.path", "s3://thor-pr-data-warehouse-fttx/fttx.ams.inventory.xdsl_port/version=0/")

		// Read FLS Data
		val fifFlsParquet = new ChronosParquetFileInputFormat[PojoFls](new Path(cfgFlsParquetLocation), createTypeInformation[PojoFls])
		fifFlsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyyMMdd", """\d{8}"""))
		fifFlsParquet.setNestedFileEnumeration(true)
		val streamFlsParquet: DataStream[RawFls] = streamEnv
				.readFile(fifFlsParquet, cfgFlsParquetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval, cfgEnrichmentScanConsistency)
        		.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_FLS_RAW)
				.map(RawFls(_))
				.filter(!_.equals(RawFls.UNKNOWN))
				.name("FLS parquet")

		// Read AMS Data
		val fifAmsParquet = new ChronosParquetFileInputFormat[PojoAms](new Path(cfgAmsParquetLocation), createTypeInformation[PojoAms])
		fifAmsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyyMMdd", """\d{8}"""))
		fifAmsParquet.setNestedFileEnumeration(true)
		val streamAmsParquet: DataStream[RawAms] = streamEnv
				.readFile(fifAmsParquet, cfgAmsParquetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval, cfgEnrichmentScanConsistency)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_AMS_RAW)
				.map(RawAms(_))
				.filter(r => r.dslam != null && !r.dslam.isEmpty)
				.filter(r => r.uni_prid != null && r.uni_prid.startsWith("UNI"))
				.name("AMS parquet")

		// Join AMS with FLS
		val streamAmsFlsParquet = streamAmsParquet.connect(streamFlsParquet)
				.keyBy(_.uni_prid, _.uni_prid)
				.flatMap(new MergeAmsFls)
				.uid(OperatorId.AMS_FLS_MERGER)
				.name("Merge AMS & FLS parquet")

		// Read Nac Data
		val cfgNacParquetLocation = appConfig.get("sources.nac-parquet.path", "s3://assn-csa-prod-telemetry-data-lake/PROD/RAW/NAC/NAC_ports/version=0/")

		val fifNacParquet = new ChronosParquetFileInputFormat[PojoNac](new Path(cfgNacParquetLocation), createTypeInformation[PojoNac])
		fifNacParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyy-MM-dd", """\d{4}-\d{2}-\d{2}"""))
		fifNacParquet.setNestedFileEnumeration(true)
		val streamNacParquet: DataStream[EnrichmentRecord] = streamEnv
				.readFile(fifNacParquet, cfgNacParquetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval, cfgEnrichmentScanConsistency)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_NAC_RAW)
        		.filter(!_.rtx_attainable_net_data_rate_ds.equals("0.0"))
				.map(EnrichmentRecord(_))
				.name("Nac parquet")

		// Read Atten375 Data from Feature Set
		val cfgChronosFeatureSetLocation = appConfig.get("sources.float-featureset-parquet.path", "s3://assn-csa-prod-telemetry-data-lake/PROD/FEATURES/fttxFeatureSet/valType=float/")
		val fifChronosFeatureSet =
			new ChronosParquetFileInputFormat[PojoChronosFeatureSetFloat](
				new Path(cfgChronosFeatureSetLocation), createTypeInformation[PojoChronosFeatureSetFloat])
		fifChronosFeatureSet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyy-MM-dd", """\d{4}-\d{2}-\d{2}"""))
		fifChronosFeatureSet.setNestedFileEnumeration(true)
		val streamAtten375Parquet: DataStream[EnrichmentRecord] = streamEnv
				.readFile(fifChronosFeatureSet, cfgChronosFeatureSetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval, cfgEnrichmentScanConsistency)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_ATTEN375_RAW)
				.map(EnrichmentRecord(_))
				.name("Atten375 parquet")

		return streamAmsFlsParquet
				.union(streamNacParquet).union(streamAtten375Parquet)
	}

	def readPercentilesTable(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment) = {
		val cfgEnrichmentScanInterval = appConfig.getLong("sources.enrichment.scan-interval", 60000L)
		val cfgEnrichmentIgnoreOlderThan = appConfig.getLong("sources.enrichment.ignore-files-older-than-days", 1)
		val cfgPctlsJsonLocation = appConfig.get("sources.percentiles-json.path", "s3://assn-csa-prod-telemetry-data-lake/PROD/REFERENCE/AttNDRPercentilesLatest/")

		val fifPercentiles = new TextInputFormat(new Path(cfgPctlsJsonLocation))
		fifPercentiles.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyy-MM-dd", """\d{4}-\d{2}-\d{2}"""))
		fifPercentiles.setNestedFileEnumeration(true)
		val streamPctls = streamEnv
				.readFile(fifPercentiles, cfgPctlsJsonLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval)
				.setParallelism(1)
				.uid(OperatorId.SOURCE_PERCENTILES_RAW)
				.flatMap(new ParsePercentilesRecord())
		streamPctls
	}

	def readEnrichmentStream(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[EnrichmentRecord] = {
		val cfgFlsInitialLocation = appConfig.get("sources.fls-initial.path", "file:///Users/Huyen/Desktop/SDC/enrich/")
		val cfgFlsIncrementalLocation = appConfig.get("sources.fls-incremental.path", "file:///Users/Huyen/Desktop/SDCTest/enrich/")

		val ifFlsInitialCsv = new TextInputFormat(
			new org.apache.flink.core.fs.Path(cfgFlsInitialLocation))
		ifFlsInitialCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
		ifFlsInitialCsv.setNestedFileEnumeration(true)
		val streamFlsInitial: DataStream[String] =
			streamEnv.readFile(ifFlsInitialCsv, cfgFlsIncrementalLocation,
				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
					.uid(OperatorId.SOURCE_FLS_INITIAL)
					.name("Initial FLS stream")

		val ifFlsIncrementalCsv = new TextInputFormat(
			new org.apache.flink.core.fs.Path(cfgFlsIncrementalLocation))
		ifFlsIncrementalCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
		ifFlsIncrementalCsv.setNestedFileEnumeration(true)
		val streamFlsIncremental: DataStream[String] =
			streamEnv.readFile(ifFlsIncrementalCsv, cfgFlsInitialLocation,
				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
					.uid(OperatorId.SOURCE_FLS_INCREMENTAL)
					.name("Incremental FLS stream")

		val flsRaw = streamFlsIncremental.union(streamFlsInitial)
				.flatMap(line => {
					val v = line.split(",")
					List(EnrichmentRecord(v(4).toLong,v(1),v(2),Map(EnrichmentAttributeName.AVC -> v(3),EnrichmentAttributeName.CPI -> v(4))))
				})

		return flsRaw
	}

	def readHistoricalData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): (DataStream[DslamMetadata], DataStream[SdcRawHistorical]) = {
		val cfgSdcHistoricalLocation = appConfig.get("sources.sdc-historical.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/")
		val cfgSdcHistoricalScanInterval = appConfig.getLong("sources.sdc-historical.scan-interval", 10000L)
		val cfgSdcHistoricalScanConsistency = appConfig.getLong("sources.sdc-historical.scan-consistency-offset", 2000L)
		val cfgSdcHistoricalIgnore = appConfig.getLong("sources.sdc-historical.ignore-files-older-than-minutes", 10000) * 60 * 1000

		implicit val hParser: SdcParser[SdcRawHistorical] = SdcRawHistorical

		val pathSdcHistorical = new org.apache.flink.core.fs.Path(cfgSdcHistoricalLocation)
		val fifSdcHistorical = new SdcTarInputFormat(pathSdcHistorical, false, "Historical")
		fifSdcHistorical.setFilesFilter(new SdcFilePathFilter(cfgSdcHistoricalIgnore))
		fifSdcHistorical.setNestedFileEnumeration(true)
		val streamDslamHistorical = streamEnv
				.readFile(fifSdcHistorical,
					cfgSdcHistoricalLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcHistoricalScanInterval, cfgSdcHistoricalScanConsistency)
				.uid(OperatorId.SOURCE_SDC_HISTORICAL)
				.assignTimestampsAndWatermarks(new DslamRecordTimeAssigner[String])
				.name("Read SDC Historical")
//		val streamSdcRawHistorical: DataStream[SdcRawHistorical] = streamDslamHistorical
//				.flatMap(new ParseSdcRecord[SdcRawHistorical]())
//				.uid(OperatorId.SDC_PARSER_HISTORICAL)
//				.name("Parse SDC Historical")

		return (streamDslamHistorical.map(r => r.metadata).uid("Metadata_Extract_Historical"),
				streamDslamHistorical)
	}

	def readInstantData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): (DataStream[DslamMetadata], DataStream[SdcRawInstant]) = {
		val cfgSdcInstantLocation = appConfig.get("sources.sdc-instant.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/instant")
		val cfgSdcInstantScanInterval = appConfig.getLong("sources.sdc-instant.scan-interval", 10000L)
		val cfgSdcInstantScanConsistency = appConfig.getLong("sources.sdc-instant.scan-consistency-offset", 2000L)
		val cfgSdcInstantIgnore = appConfig.getLong("sources.sdc-instant.ignore-files-older-than-minutes", 10000) * 60 * 1000

		implicit val iParser: SdcParser[SdcRawInstant] = SdcRawInstant

		val pathSdcInstant = new org.apache.flink.core.fs.Path(cfgSdcInstantLocation)
		val fifSdcInstant = new SdcTarInputFormat(pathSdcInstant, true, "Instant")
		fifSdcInstant.setFilesFilter(new SdcFilePathFilter(cfgSdcInstantIgnore))
		fifSdcInstant.setNestedFileEnumeration(true)
		val streamDslamInstant = streamEnv
				.readFile(fifSdcInstant,
					cfgSdcInstantLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY,
					cfgSdcInstantScanInterval, cfgSdcInstantScanConsistency)
				.uid(OperatorId.SOURCE_SDC_INSTANT)
				.assignTimestampsAndWatermarks(new DslamRecordTimeAssigner[String])
				.name("Read SDC Instant")
//		val streamSdcRawInstant: DataStream[SdcRawInstant] = streamDslamInstant
//				.flatMap(new ParseSdcRecord[SdcRawInstant])
//				.uid(OperatorId.SDC_PARSER_INSTANT)
//				.name("Parse SDC Instant")
//				.filter(r => !r.data.if_admin_status.equalsIgnoreCase("down"))
//				.uid(OperatorId.SDC_PARSER_INSTANT + "_1")

		return (streamDslamInstant.map(_.metadata).uid("Metadata_Extract_Instant"),
				streamDslamInstant)
	}

	def readMonitoringData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[(String, Boolean)] = {
		val path = appConfig.get("source.nosync-candidate.path")
		streamEnv.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.filter(_.startsWith("AVC"))
        		.map{r =>
					val (avc, flag) = r.span(!_.equals(','))
					(avc, flag.equals(",on"))
				}
	}

	def readHistoricalDataFromES(appConfig: ParameterTool,
								 streamEnv: StreamExecutionEnvironment,
								 streamInput: DataStream[(String, Boolean)]): (DataStream[SdcCompact], DataStream[((String, String), Boolean)], DataStream[(String, Boolean)]) = {
		val esUrl = appConfig.get("sink.elasticsearch.endpoint")
		val indexName = s"${appConfig.get("sink.elasticsearch.sdc.index-name", "sdc-counters-prod")}*"
		val docType = appConfig.get("sink.elasticsearch.sdc.doc-type", "counter")
		val recordsCount = appConfig.getInt("source.nosync-candicate.measurements-count", 96)
		val timeout = appConfig.getInt("source.nosync-candicate.timeout-seconds", 3)

		val streamEsSearchResponse = AsyncDataStream.unorderedWait(streamInput,
			new ReadHistoricalDataFromES(esUrl, indexName, docType, recordsCount, timeout), timeout + 5, TimeUnit.SECONDS, 100)

		val tagToggle = OutputTag[((String, String), Boolean)]("NoSyncTogglePhysicalRef")
		val tagRetry = OutputTag[(String, Boolean)]("NoSyncToggleRetry")

		val streamEsData = streamEsSearchResponse.process(new ParseEsQueryResult(tagToggle, tagRetry))

		(streamEsData, streamEsData.getSideOutput(tagToggle), streamEsData.getSideOutput(tagRetry))
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

		/** Read fls, ams, and merge them to create enrichment stream */
		val streamEnrichmentAgg = readEnrichmentData(appConfig, streamEnv)
		//  		.union(readEnrichmentStream(appConfig, streamEnv))

		/** Read SDC streams */
		val (streamDslamMetadataInstant, streamDslamInstant) = readInstantData(appConfig, streamEnv)
		val (streamDslamMetadataHistorical, streamDslamHistorical) = readHistoricalData(appConfig, streamEnv)

		/** Find missing DSLAM */
		lazy val streamDslamMissingInstant = streamDslamMetadataInstant
				.keyBy(_.name)
				.window(EventTimeSessionWindows.withGap(Time.minutes(2 * cfgRollingAverageSlideInterval)))
				.allowedLateness(Time.minutes(cfgRollingAverageAllowedLateness))
				.aggregate(new IdentifyMissingDslam.AggIncremental, new IdentifyMissingDslam.AggFinal)
				.uid(OperatorId.STATS_MISSING_DSLAM_INSTANT)
				.name("Identify missing Dslam - Instant")

		lazy val streamDslamMissingHistorical = streamDslamMetadataHistorical
				.keyBy(_.name)
				.window(EventTimeSessionWindows.withGap(Time.minutes(2 * cfgRollingAverageSlideInterval)))
				.allowedLateness(Time.minutes(cfgRollingAverageAllowedLateness))
				.aggregate(new IdentifyMissingDslam.AggIncremental, new IdentifyMissingDslam.AggFinal)
				.uid(OperatorId.STATS_MISSING_DSLAM_HISTORICAL)
				.name("Identify missing Dslam - Historical")

		/** Enrich SDC Streams */
		val outputTagI = OutputTag[SdcEnrichedInstant]("enriched_i")
		val outputTagH = OutputTag[SdcEnrichedHistorical]("enriched_h")

//		val streamEnriched: DataStream[SdcEnrichedBase] =
//			streamDslamInstant.map(_.asInstanceOf[SdcRawBase]).uid("Huyen1")
//					.union(streamDslamHistorical.map(_.asInstanceOf[SdcRawBase]).uid("Huyen2"))
//					.keyBy(r => (r.dslam, r.port))
//					.connect(streamEnrichmentAgg.keyBy(r => (r.dslam, r.port)))
//					.process(new EnrichSdcRecord(outputTagI, outputTagH))
//					.uid(OperatorId.SDC_ENRICHER)
//					.name("Enrich SDC records")

		val pctlsStateDescriptor = new MapStateDescriptor(
			"PercentilessBroadcastState",
			BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO);
		val streamPctls = readPercentilesTable(appConfig, streamEnv)
		val streamPctlsBroadcast = streamPctls.broadcast(pctlsStateDescriptor)

		val streamEnriched: DataStream[SdcEnrichedBase] =
			streamDslamInstant.map(_.asInstanceOf[CopperLine]).uid("Huyen1")
					.union(streamDslamHistorical.map(_.asInstanceOf[CopperLine]).uid("Huyen2"))
					.union(streamEnrichmentAgg.map(_.asInstanceOf[CopperLine]).uid("Huyen3"))
					.keyBy(r => (r.dslam, r.port))
					.connect(streamPctlsBroadcast)
					.process(new EnrichSdcRecord(outputTagI, outputTagH))
					.uid(OperatorId.SDC_ENRICHER)
					.name("Enrich SDC records")


		/** split SDC streams into instant and historical */
		val streamEnrichedInstant = streamEnriched.getSideOutput(outputTagI)
		val streamEnrichedHistorical = streamEnriched.getSideOutput(outputTagH)

		/** Handling Nosync */
		val streamNosyncCandidate = readMonitoringData(appConfig, streamEnv)
		val (streamNosyncEsData, streamNosyncToggle, streamNosyncRetry) =
			readHistoricalDataFromES(appConfig, streamEnv, streamNosyncCandidate)

		val streamNosyncData = new DataStreamUtils(streamEnrichedHistorical.map(r => SdcCombined(r)))
				.reinterpretAsKeyedStream(r => (r.dslam, r.port))
        		.connect(streamNosyncToggle.keyBy(r => r._1))
        		.flatMap(new NosyncCandicateFilter())
        		.union(streamNosyncEsData)

		/** Output */
		if (debug) {
			if (cfgParquetEnabled) {
				streamEnrichedInstant.addSink(
					SdcParquetFileSink.buildSinkGeneric[SdcEnrichedInstant](SdcEnrichedInstant.getSchema(),
						cfgParquetInstantPath, cfgParquetPrefix, cfgParquetSuffixFormat))
						.setParallelism(cfgElasticSearchAverageParallelism)
				streamEnrichedHistorical.addSink(
					SdcParquetFileSink.buildSinkGeneric[SdcEnrichedHistorical](SdcEnrichedHistorical.getSchema(),
						cfgParquetHistoricalPath, cfgParquetPrefix, cfgParquetSuffixFormat))
						.setParallelism(cfgElasticSearchAverageParallelism)
			}
//			streamDslamHistorical.print()
			//			streamDslamInstant.print()
			//			streamEnrichment.print()
//			streamEnrichedInstant.print()
//			streamEnriched.print()
//			streamPctls.print()
			//			streamEnrichment.map(_.toString).print()
			//			streamEnriched.print()
			//			streamNotEnrichable.print()
			//			if (cfgElasticSearchAverageEnabled) streamAverage.filter(s => s.enrich.avcId.equals("AVC000066198558")).print()
			//			streamDslamInstant.map(_.toString).addSink(SdcParquetFileSink.buildSink("file:///Users/Huyen/Desktop/SDCTest/output"))
			//			streamDslamInstant.addSink(SdcParquetFileSink.buildSink[SdcRawInstant](cfgParquetInstantPath, cfgParquetPrefix, cfgParquetSuffixFormat))
			//			streamEnriched
			//					.addSink(SdcElasticSearchSink.createSink[SdcEnriched](cfgElasticSearchEndpoint,
			//						cfgElasticSearchCombinedSdcIndexName,
			//						cfgElasticSearchBulkInterval,
			//						cfgElasticSearchMaxRetries))
			//					.uid(OperatorId.SINK_ELASTIC_COMBINED)
			//			streamEnrichedInstant.print()
			//			averageInstant.print()
			//			combined.print()
			//			combined.map(r=>r.toString).addSink(ElasticSearch.createSink())
			//			sdcInErr.print()
		} else {
			if (cfgElasticSearchCombinedSdcEnabled)
				streamEnriched
						.addSink(SdcElasticSearchSink.createUpdatableSdcElasticSearchSink[SdcEnrichedBase](
							cfgElasticSearchEndpoint,
							cfgElasticSearchCombinedSdcIndexName,
							cfgElasticSearchBulkActions,
							cfgElasticSearchBulkSize,
							cfgElasticSearchBulkInterval,
							cfgElasticSearchBackoffDelay,
							cfgElasticSearchMaxRetries))
						.setParallelism(cfgElasticSearchCombinedSdcParallelism)
						.uid(OperatorId.SINK_ELASTIC_COMBINED)
						.name("ES - Combined")

			if (cfgElasticSearchEnrichmentEnabled)
				streamEnrichmentAgg
						.addSink(SdcElasticSearchSink.createElasticSearchSink[EnrichmentRecord](
							cfgElasticSearchEndpoint,
							r => cfgElasticSearchEnrichmentIndexName,
							r => r.toMap,
							r => s"${r.dslam}_${r.port}",
							cfgElasticSearchBulkActions,
							cfgElasticSearchBulkSize,
							cfgElasticSearchBulkInterval,
							cfgElasticSearchBackoffDelay,
							cfgElasticSearchMaxRetries))
						.setParallelism(cfgElasticSearchEnrichmentParallelism)
						.uid(OperatorId.SINK_ELASTIC_ENRICHMENT)
						.name("ES - Enrichment")

			//			if (cfgElasticSearchUnenrichableEnabled)
			//				streamNotEnrichableHistorical
			//					.addSink(SdcElasticSearchSink.createSdcSink[SdcRawHistorical](
			//						cfgElasticSearchEndpoint,
			//						cfgElasticSearchUnenrichableIndexName,
			//						cfgElasticSearchBulkInterval,
			//						cfgElasticSearchMaxRetries,
			//						SdcElasticSearchSink.DAILY_INDEX))
			//					.setParallelism(cfgElasticSearchUnenrichableParallelism)
			//					.uid(OperatorId.SINK_ELASTIC_UNENRICHABLE)
			//					.name("ES - Unenrichable")

			if (cfgParquetEnabled) {
				streamEnrichedInstant.addSink(
					SdcParquetFileSink.buildSinkGeneric[SdcEnrichedInstant](SdcEnrichedInstant.getSchema(),
						cfgParquetInstantPath, cfgParquetPrefix, cfgParquetSuffixFormat))
						.setParallelism(cfgParquetParallelism)
						.uid(OperatorId.SINK_PARQUET_INSTANT)
						.name("S3 - Instant")

				streamEnrichedHistorical.addSink(
					SdcParquetFileSink.buildSinkGeneric[SdcEnrichedHistorical](SdcEnrichedHistorical.getSchema(),
						cfgParquetHistoricalPath, cfgParquetPrefix, cfgParquetSuffixFormat))
						.setParallelism(cfgParquetParallelism)
						.uid(OperatorId.SINK_PARQUET_HISTORICAL)
						.name("S3 - Historical")

				streamDslamMetadataInstant.union(streamDslamMetadataHistorical).addSink(
					SdcParquetFileSink.buildSinkGeneric[DslamMetadata](DslamMetadata.getSchema(),
						cfgParquetMetadataPath, cfgParquetPrefix, cfgParquetSuffixFormat))
						.setParallelism(1)
						.uid(OperatorId.SINK_PARQUET_METADATA)
						.name("S3 - Metadata")
			}

			if (cfgElasticSearchStatsEnabled) {
				streamDslamMetadataInstant.union(streamDslamMetadataHistorical)
						.addSink(SdcElasticSearchSink.createElasticSearchSink[DslamMetadata](
							cfgElasticSearchEndpoint,
							r => s"${cfgElasticSearchStatsDslamMetaIndexName}_${SdcElasticSearchSink.MONTHLY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r.ts))}",
							DslamMetadata.toMap,
							r => s"${if (r.isInstant) "I" else "H"}_${r.name}_${r.ts}",
							1000,
							5,
							cfgElasticSearchBulkInterval,
							cfgElasticSearchBackoffDelay,
							3000))
						.setParallelism(1)
						.uid(OperatorId.SINK_ELASTIC_METADATA)
						.name("ES - DSLAM Info")

				streamDslamMissingInstant
						.addSink(SdcElasticSearchSink.createElasticSearchSink[(Long, String)](
							cfgElasticSearchEndpoint,
							r => s"${cfgElasticSearchStatsMissingInstantIndexName}_${SdcElasticSearchSink.MONTHLY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r._1))}",
							r => Map("metrics_timestamp" -> (r._1 + cfgRollingAverageSlideInterval * 60L * 1000L), "dslam" -> r._2),
							r => s"I_${r._2}_${r._1}",
							1000,
							5,
							cfgElasticSearchBulkInterval,
							cfgElasticSearchBackoffDelay,
							3000))
						.setParallelism(1)
						.uid(OperatorId.SINK_ELASTIC_MISSING_I)
						.name("ES - Missing DSLAM - Instant")

				streamDslamMissingHistorical
						.addSink(SdcElasticSearchSink.createElasticSearchSink[(Long, String)](
							cfgElasticSearchEndpoint,
							r => s"${cfgElasticSearchStatsMissingHistoricalIndexName}_${SdcElasticSearchSink.MONTHLY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r._1))}",
							r => Map("metrics_timestamp" -> (r._1 + cfgRollingAverageSlideInterval * 60L * 1000L), "dslam" -> r._2),
							r => s"H_${r._2}_${r._1}",
							1000,
							5,
							cfgElasticSearchBulkInterval,
							cfgElasticSearchBackoffDelay,
							3000))
						.setParallelism(1)
						.uid(OperatorId.SINK_ELASTIC_MISSING_H)
						.name("ES - Missing DSLAM - Historical")

			}
		}
		// execute program
		streamEnv.execute("Chronos SDC")
	}
}