package com.starfox.analysis.copper.sdc.flink

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.starfox.analysis.copper.sdc.data._
import com.starfox.analysis.copper.sdc.flink.cep.NoSync
import com.starfox.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.ReadHistoricalDataFromES
import com.starfox.analysis.copper.sdc.flink.operator._
import com.starfox.analysis.copper.sdc.flink.sink.{SdcElasticSearchSink, SdcKinesisProducer, SdcParquetFileSink}
import com.starfox.analysis.copper.sdc.flink.source.{KafkaConsumer, _}
import com.starfox.analysis.copper.sdc.data
import com.starfox.analysis.copper.sdc.data._
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.IterativeStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object StreamingSdc {
	def initEnvironment(appConfig: ParameterTool): StreamExecutionEnvironment = {
		val cfgParallelism = appConfig.getInt("parallelism", 32)
		val cfgTaskTimeoutInterval = appConfig.getLong("task-timeout-interval", 30000)
		val cfgCheckpointEnabled = appConfig.getBoolean("checkpoint.enabled", true)
		val cfgCheckpointLocation = appConfig.get("checkpoint.path", "s3://assn-csa-prod-telemetry-data-lake/TestData/Spike/Huyen/SDC/ckpoint/")
		val cfgCheckpointInterval = appConfig.getLong("checkpoint.interval", 800000L)
		val cfgCheckpointTimeout = appConfig.getLong("checkpoint.timeout", 600000L)
		val cfgCheckpointMinPause = appConfig.getLong("checkpoint.minimum-pause", 600000L)
		val cfgCheckpointStopJobWhenFail = appConfig.getBoolean("checkpoint.stop-job-when-fail", true)

		val streamEnv =
			if (cfgCheckpointEnabled) StreamExecutionEnvironment.getExecutionEnvironment
					.setStateBackend(new RocksDBStateBackend(cfgCheckpointLocation, true))
					.enableCheckpointing(cfgCheckpointInterval, CheckpointingMode.EXACTLY_ONCE, true)
			else StreamExecutionEnvironment.getExecutionEnvironment

		if (cfgCheckpointEnabled) {
			streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(cfgCheckpointMinPause)
			streamEnv.getCheckpointConfig.setCheckpointTimeout(cfgCheckpointTimeout)
			streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(cfgCheckpointStopJobWhenFail)
			streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
		}
		streamEnv.getConfig
				.setTaskCancellationInterval(cfgTaskTimeoutInterval)
				.enableObjectReuse()
		streamEnv.setParallelism(cfgParallelism)
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		if (!appConfig.getBoolean("debug", false)) {
			streamEnv.getConfig.disableSysoutLogging()
		}

		return streamEnv
	}

	def readEnrichmentData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[EnrichmentRecord] = {
		val cfgEnrichmentScanInterval = appConfig.getLong("sources.enrichment.scan-interval", 60000L)
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
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval)
        		.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_FLS_RAW)
				.map(RawFls(_))
				.filter(!_.equals(RawFls.UNKNOWN))
				.name("FLS parquet")
				.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[RawFls])

		// Read AMS Data
		val fifAmsParquet = new ChronosParquetFileInputFormat[PojoAms](new Path(cfgAmsParquetLocation), createTypeInformation[PojoAms])
		fifAmsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyyMMdd", """\d{8}"""))
		fifAmsParquet.setNestedFileEnumeration(true)
		val streamAmsParquet: DataStream[RawAms] = streamEnv
				.readFile(fifAmsParquet, cfgAmsParquetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_AMS_RAW)
				.map(RawAms(_))
				.filter(r => r.dslam != null && !r.dslam.isEmpty)
				.filter(r => r.uni_prid != null && r.uni_prid.startsWith("UNI"))
				.name("AMS parquet")
				.assignTimestampsAndWatermarks(EnrichmentRecordTimeExtractorForIdleSources[RawAms])

		// Join AMS with FLS
		val streamAmsFlsParquet = streamAmsParquet.connect(streamFlsParquet)
				.keyBy(_.uni_prid, _.uni_prid)
				.process(new MergeAmsFls)
				.uid(OperatorId.AMS_FLS_MERGER)
				.name("Merge AMS & FLS parquet")

		// Read Nac Data
		val cfgNacParquetLocation = appConfig.get("sources.nac-parquet.path", "s3://assn-csa-prod-telemetry-data-lake/PROD/RAW/NAC/NAC_ports/version=0/")

		val fifNacParquet = new ChronosParquetFileInputFormat[PojoNac](new Path(cfgNacParquetLocation), createTypeInformation[PojoNac])
		fifNacParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyy-MM-dd", """\d{4}-\d{2}-\d{2}"""))
		fifNacParquet.setNestedFileEnumeration(true)
		val streamNacParquet: DataStream[EnrichmentRecord] = streamEnv
				.readFile(fifNacParquet, cfgNacParquetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_NAC_RAW)
        		.filter(!_.rtx_attainable_net_data_rate_ds.equals("0.0"))
				.map(EnrichmentRecord(_))
				.name("Nac parquet")
				.assignTimestampsAndWatermarks(EnrichmentRecordTimeExtractorForIdleSources[EnrichmentRecord])

		// Read Atten375 Data from Feature Set
		val cfgChronosFeatureSetLocation = appConfig.get("sources.float-featureset-parquet.path", "s3://assn-csa-prod-telemetry-data-lake/PROD/FEATURES/fttxFeatureSet/valType=float/")
		val fifChronosFeatureSet =
			new ChronosParquetFileInputFormat[PojoChronosFeatureSetFloat](
				new Path(cfgChronosFeatureSetLocation), createTypeInformation[PojoChronosFeatureSetFloat])
		fifChronosFeatureSet.setFilesFilter(new EnrichmentFilePathFilter(cfgEnrichmentIgnoreOlderThan, "yyyy-MM-dd", """\d{4}-\d{2}-\d{2}"""))
		fifChronosFeatureSet.setNestedFileEnumeration(true)
		val streamAtten375Parquet: DataStream[EnrichmentRecord] = streamEnv
				.readFile(fifChronosFeatureSet, cfgChronosFeatureSetLocation,
					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgEnrichmentScanInterval)
				.setParallelism(cfgEnrichmentReaderParallelism)
				.uid(OperatorId.SOURCE_ATTEN375_RAW)
				.map(data.EnrichmentRecord(_))
				.name("Atten375 parquet")
				.assignTimestampsAndWatermarks(EnrichmentRecordTimeExtractorForIdleSources[EnrichmentRecord])

		return streamAmsFlsParquet.assignTimestampsAndWatermarks(EnrichmentRecordTimeExtractorForIdleSources[EnrichmentRecord])
				.union(streamNacParquet).union(streamAtten375Parquet)
	}

	def readPercentilesTable(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[(String, List[java.lang.Float])] = {
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
				.assignTimestampsAndWatermarks(ProcessingTimeExtractorForIdleSources[(String, List[java.lang.Float])])
		streamPctls
	}

	def readSharpFls(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[EnrichmentRecord] = {
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

		streamFlsIncremental.union(streamFlsInitial)
				.flatMap(line => {
					val v = line.split(",")
					List(EnrichmentRecord(v(4).toLong,v(1),v(2),Map(EnrichmentAttributeName.AVC -> v(3),EnrichmentAttributeName.CPI -> v(4))))
				})
	}


	def readNoSyncMonitoringData(appConfig: ParameterTool, streamEnv: StreamExecutionEnvironment): DataStream[(String, Boolean)] = {
		val regexRequest = """^(AVC\d{12})(?:[, ](?:(on|true)|(off|false)))?$""".r
		val path = appConfig.get("concierge.source.watchlist.path", "file:///Users/Huyen/Desktop/SDCTest/input/es")
		val ret = streamEnv.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.flatMap(_ match {
					case regexRequest(a, b, c) =>
						Seq((a, c == null))
					case _ => Seq.empty
				})
//		val kinesisStreamName = appConfig.get("source.nosync-candidate.kinesis.name", "assn-huyen")
//		val ret = KafkaConsumer.readNoSyncMonitorRequestFromKafka(streamEnv, kinesisStreamName)
		ret.assignTimestampsAndWatermarks(ProcessingTimeExtractorForIdleSources[(String, Boolean)])
	}


	def main(args: Array[String]) {

		/** Read configuration */
		val configFile = ParameterTool.fromArgs(args).get("configFile", "dev.properties")

		import java.net.URI
		val fs = org.apache.flink.core.fs.FileSystem.get(URI.create(configFile))
		val appConfig = ParameterTool.fromPropertiesFile(fs.open(new Path(configFile)))
		val debug = appConfig.getBoolean("debug", false)

		/** set up the streaming execution environment */
		val streamEnv = initEnvironment(appConfig)

		/** Read fls, ams, and merge them to create enrichment stream */
		val streamEnrichmentAgg = readEnrichmentData(appConfig, streamEnv)
		//  		.union(readEnrichmentStream(appConfig, streamEnv))

		/** Read SDC streams */
		val cfgSdcInstantLocation = appConfig.get("sources.sdc-instant.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/instant")
		val cfgSdcHistoricalLocation = appConfig.get("sources.sdc-historical.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/")
		val cfgSdcScanInterval = appConfig.getLong("sources.sdc.scan-interval", 10000L)
		val cfgSdcScanConsistency = appConfig.getLong("sources.sdc.scan-consistency-offset", 2000L)
		val cfgSdcIgnoreThreshold = appConfig.getLong("sources.sdc.ignore-files-older-than-minutes", 7*24*60L) * 60 * 1000
		val cfgSdcCombinerLateAllowness = appConfig.getLong("sources.sdc.allow-late-files-upto-minutes", 120) * 60 * 1000

		val cfgReadAfterCombine = appConfig.getBoolean("read.after.combine", false)

		val (streamDslamCombined, streamDslamUnmatched, streamDslamMetadata) =
			ReadAndCombineRawSdcFiles(streamEnv,
				cfgSdcInstantLocation,
				cfgSdcHistoricalLocation,
				cfgSdcScanInterval,
				cfgSdcScanConsistency,
				cfgSdcIgnoreThreshold,
				cfgSdcCombinerLateAllowness,
				readAfterCombine = cfgReadAfterCombine)
		val streamSdcCombinedRaw: DataStream[SdcCombined] = ParseCombinedSdcRecord(streamDslamCombined)

		/** Enrich SDC Streams */
		val streamPctls = readPercentilesTable(appConfig, streamEnv)
		val streamSdcEnriched: DataStream[SdcCombined] =
			EnrichSdcRecord(streamSdcCombinedRaw.filter(_.dataI.ifAdminStatus.booleanValue()), streamPctls, streamEnrichmentAgg)


		/** Handling Nosync */
		if (appConfig.getBoolean("concierge.enabled", false)) {
			val streamNosyncWatchListFromExternalRaw = readNoSyncMonitoringData(appConfig, streamEnv)

			val esUrl = appConfig.get("concierge.source.elasticsearch.endpoint")
			val indexName = s"${appConfig.get("concierge.source.elasticsearch.sdc.index-name", appConfig.get("sink.elasticsearch.sdc.index-name", "copper-sdc-combined-default"))}*"
			val docType = appConfig.get("source.elasticsearch.sdc.doc-type", "_doc")
			val esQueryTimeout = appConfig.getInt("concierge.source.elasticsearch.timeout-seconds", 5)
			val reInitMonitorPeriod = appConfig.getInt("concierge.reinit-monitor.period", 288)
			val reInitMonitorThreshold = appConfig.getInt("concierge.reinit-monitor.threshold", 5)
			val uasMonitorPeriod = appConfig.getInt("concierge.uas-monitor.period", 8)
			val uasMonitorThreshold = appConfig.getInt("concierge.uas-monitor.threshold", 10)

			val (streamNosyncEsData, streamWatchListFromExternal, streamWatchListRetry) =
				ReadHistoricalDataFromES(streamNosyncWatchListFromExternalRaw, esUrl, indexName, docType, reInitMonitorPeriod, esQueryTimeout)

			val conciergeExportWatchlistTriggerPath = appConfig.get("concierge.export-watchlist.trigger-path", "file:///Users/Huyen/Desktop/SDCTest/input/export")
			val exportTrigger = streamEnv.readFile(new TextInputFormat(new Path(conciergeExportWatchlistTriggerPath)),
				conciergeExportWatchlistTriggerPath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
					.map(!_.equals(""))

			val streamNoSyncOutput = streamWatchListFromExternal.iterate((watchList: DataStream[((String, String), Boolean)]) => {
				val tmpInRight: DataStream[Either[((String, String), Boolean), SdcCompact]] = streamSdcEnriched
						.map(r => Right[((String, String), Boolean), SdcCompact](SdcCompact(r)))
				val tmpInLeft: DataStream[Either[((String, String), Boolean), SdcCompact]] = watchList
						.keyBy(_._1).map(r => Left[((String, String), Boolean), SdcCompact](r))
				val tmpIn: DataStream[Either[((String, String), Boolean), SdcCompact]]  = tmpInRight.union(tmpInLeft)

				val tmpInKeyed = new DataStreamUtils(tmpIn).reinterpretAsKeyedStream(r => r match {
					case Left(t) => t._1
					case Right(e) => (e.dslam, e.port)
				})

				val (streamNosyncInputLive, exported) = ExportWatchList(tmpInKeyed, exportTrigger)
				val streamNosyncInput = streamNosyncEsData.keyBy(r => (r.dslam, r.port)).map(r => r)
						.union(streamNosyncInputLive)

				val streamNosyncOutput =
					NoSync(new DataStreamUtils(streamNosyncInput).reinterpretAsKeyedStream(r => (r.dslam, r.port)),
						reInitMonitorPeriod, uasMonitorPeriod,
						reInitMonitorThreshold, uasMonitorThreshold)

				(streamNosyncOutput.map(r => ((r.dslam, r.port), false)),
						streamNosyncOutput)
			})

			//
			//		val streamNoSyncOutput = streamWatchListFromExternal.iterate((watchList: DataStream[((String, String), Boolean)]) => {
			//			val streamNosyncInput = new DataStreamUtils(streamSdcEnriched).reinterpretAsKeyedStream(r => (r.dslam, r.port))
			//					.connect(watchList.keyBy(_._1))
			//					.flatMap(new NosyncCandidateFilter())
			//					.union(streamNosyncEsData.keyBy(r => (r.dslam, r.port)).map(r => r))
			//			val streamNosyncOutput =
			//				NoSync(new DataStreamUtils(streamNosyncInput).reinterpretAsKeyedStream(r => (r.dslam, r.port)),
			//					reInitMonitorPeriod, uasMonitorPeriod,
			//					reInitMonitorThreshold, uasMonitorThreshold)
			//
			//			(streamNosyncOutput.map(r => ((r.dslam, r.port), false)),
			//					streamNosyncOutput)
			//		})

			//		val kinesisStreamName = appConfig.get("sink.kinesis.concierge.stream-name", "assn-huyen")
			//		streamNoSyncOutput.addSink(SdcKinesisProducer[SdcCompact](kinesisStreamName)).name("KinesisSink")

			streamNoSyncOutput.map(_.toString).addSink(StreamingFileSink
					.forRowFormat(new Path("s3://assn-csa-prod-telemetry-data-lake/TEMP/Huyen/SDC/nosyncOut"),
						new SimpleStringEncoder[String]("UTF-8"))
					.withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd"))
					.build())
		}




		/** Output */
		val cfgParquetEnabled = appConfig.getBoolean("sink.parquet.enabled", false)
		if (cfgParquetEnabled) {
			val cfgParquetParallelism = appConfig.getInt("sink.parquet.parallelism", 4)
			val cfgParquetPrefix = appConfig.get("sink.parquet.prefix", "")
			val cfgParquetSuffixFormat = appConfig.get("sink.parquet.suffixFormat", "yyyy-MM-dd-hh")
			val cfgParquetInstantPath = appConfig.get("sink.parquet.sdc.path", "file:///Users/Huyen/Desktop/SDCTest/output/sdc")
			val cfgParquetMetadataPath = appConfig.get("sink.parquet.metadata.path", "file:///Users/Huyen/Desktop/SDCTest/output/metadata")
			val cfgCsvUnmatchedPath = appConfig.get("sink.csv.unmatched.path", "file:///Users/Huyen/Desktop/SDCTest/output/metadata")

			streamSdcEnriched.addSink(
				SdcParquetFileSink.buildSinkGeneric[SdcCombined](SdcCombined.SCHEMA,
					cfgParquetInstantPath, cfgParquetPrefix, cfgParquetSuffixFormat))
					.setParallelism(cfgParquetParallelism)
					.uid(OperatorId.SINK_PARQUET_COMBINED)
					.name("S3 - SDC")

			streamDslamMetadata.addSink(
				SdcParquetFileSink.buildSinkGeneric[DslamRaw[None.type]](DslamRaw.SCHEMA,
					cfgParquetMetadataPath, cfgParquetPrefix, cfgParquetSuffixFormat))
					.setParallelism(1)
					.uid(OperatorId.SINK_PARQUET_METADATA)
					.name("S3 - Metadata")

			streamDslamUnmatched.map(r => s"${r.name},${r.ts},${r.dslamType},${r.metadata.relativePath}")
					.addSink(StreamingFileSink
							.forRowFormat(new Path(cfgCsvUnmatchedPath), new SimpleStringEncoder[String]("UTF-8"))
							.withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd"))
							.build())
		}

		val cfgElasticSearchEndpoint = appConfig.get("sink.elasticsearch.endpoint", "vpc-assn-dev-chronos-devops-pmwehr2e7xujnadk53jsx4bjne.ap-southeast-2.es.amazonaws.com")
		val cfgElasticSearchBulkActions = appConfig.getInt("sink.elasticsearch.bulk-actions", 10000)
		val cfgElasticSearchBulkSize = appConfig.getInt("sink.elasticsearch.bulk-size-mb", 10)
		val cfgElasticSearchMaxRetries = appConfig.getInt("sink.elasticsearch.max-retries-in-nine-minutes", 3 * cfgElasticSearchBulkActions)
		val cfgElasticSearchBulkInterval = appConfig.getInt("sink.elasticsearch.bulk-interval-ms", 60000)
		val cfgElasticSearchBackoffDelay = appConfig.getInt("sink.elasticsearch.bulk-flush-backoff-delay-ms", 100)

		if (appConfig.getBoolean("sink.elasticsearch.sdc.enabled", false)) {
			val cfgElasticSearchCombinedSdcIndexName = appConfig.get("sink.elasticsearch.sdc.index-name", "copper-sdc-combined-default")
			val cfgElasticSearchCombinedSdcParallelism = appConfig.getInt("sink.elasticsearch.sdc.parallelism", 4)
			streamSdcEnriched
					.addSink(SdcElasticSearchSink.createUpdatableSdcElasticSearchSink[SdcCombined](
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
		}

		if (appConfig.getBoolean("sink.elasticsearch.enrichment.enabled", false)) {
			val cfgElasticSearchEnrichmentIndexName = appConfig.get("sink.elasticsearch.enrichment.index-name", "copper-enrichment-default")
			val cfgElasticSearchEnrichmentParallelism = appConfig.getInt("sink.elasticsearch.enrichment.parallelism", 1)
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
		}

		if (appConfig.getBoolean("sink.elasticsearch.stats.enabled", false)) {
			val cfgElasticSearchStatsDslamMetaIndexName = appConfig.get("sink.elasticsearch.stats.dslam-meta.index-name", "copper-sdc-dslam-metadata")
			streamDslamMetadata
					.addSink(SdcElasticSearchSink.createElasticSearchSink[DslamRaw[None.type ]](
						cfgElasticSearchEndpoint,
						r => s"${cfgElasticSearchStatsDslamMetaIndexName}_${SdcElasticSearchSink.MONTHLY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r.ts))}",
						DslamRaw.toMap,
						r => s"${if (r.dslamType == DslamType.DSLAM_INSTANT) "I" else "H"}_${r.name}_${r.ts}",
						1000,
						5,
						cfgElasticSearchBulkInterval,
						cfgElasticSearchBackoffDelay,
						3000))
					.setParallelism(1)
					.uid(OperatorId.SINK_ELASTIC_METADATA)
					.name("ES - DSLAM Info")
		}

		if (appConfig.getBoolean("sink.elasticsearch.unmatched.enabled", false)) {
			val cfgElasticSearchUnmatchedDslamIndexName = appConfig.get("sink.elasticsearch.stats.dslam-unmatched.index-name", "copper-sdc-dslam-unmatched")
			streamDslamUnmatched
					.addSink(SdcElasticSearchSink.createElasticSearchSink[DslamRaw[None.type ]](
						cfgElasticSearchEndpoint,
						r => s"${cfgElasticSearchUnmatchedDslamIndexName}_${SdcElasticSearchSink.MONTHLY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r.ts))}",
						DslamRaw.toMap,
						r => s"${if (r.dslamType == DslamType.DSLAM_INSTANT) "I" else "H"}_${r.name}_${r.ts}",
						1000,
						5,
						cfgElasticSearchBulkInterval,
						cfgElasticSearchBackoffDelay,
						3000))
					.setParallelism(1)
					.uid(OperatorId.SINK_ELASTIC_METADATA)
					.name("ES - DSLAM Info")
		}

		streamEnv.execute("Chronos SDC")
	}
}