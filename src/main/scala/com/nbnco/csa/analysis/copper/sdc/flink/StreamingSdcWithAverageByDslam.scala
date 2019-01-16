package com.nbnco.csa.analysis.copper.sdc.flink

import com.nbnco.csa.analysis.copper.sdc.data._
import com.nbnco.csa.analysis.copper.sdc.flink.operator._
import com.nbnco.csa.analysis.copper.sdc.flink.sink.{SdcElasticSearchSink, SdcParquetFileSink}
import com.nbnco.csa.analysis.copper.sdc.flink.source._
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
//import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisProducer, KinesisPartitioner}

object StreamingSdcWithAverageByDslam {
	object OperatorId {
		val SOURCE_SDC_INSTANT = "Source_Sdc_Instant"
		val SOURCE_SDC_HISTORICAL = "Source_Sdc_Historical"
		val SOURCE_FLS_INITIAL = "Source_Fls_Initial"
		val SOURCE_FLS_INCREMENTAL = "Source_Fls_Incremental"
		val SOURCE_FLS_RAW = "Source_Fls_Raw"
		val SOURCE_AMS_RAW = "Source_Ams_Raw"
		val SINK_ELASTIC_ENRICHMENT = "Elastic_Enrichment"
		val SINK_ELASTIC_COMBINED = "Elastic_Combined"
		val SINK_ELASTIC_AVERAGE = "Elastic_Average"
		val SINK_ELASTIC_UNENRICHABLE = "Elastic_Unenrichable"
		val SINK_ELASTIC_METADATA = "Elastic_Dslam_Metadata"
		val SINK_ELASTIC_MISSING_I = "Elastic_Dslam_Missing_Instant"
		val SINK_ELASTIC_MISSING_H = "Elastic_Dslam_Missing_Historical"
		val SINK_PARQUET_INSTANT = "S3_Parquet_Instant"
		val SINK_PARQUET_HISTORICAL = "S3_Parquet_Historical"
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

	def main(args: Array[String]) {

		/** Read configuration */
		val configFile = ParameterTool.fromArgs(args).get("configFile", "dev.properties")
		val appConfig = ParameterTool.fromPropertiesFile(configFile)
		val debug = appConfig.getBoolean("debug", false)

		val cfgParallelism = appConfig.getInt("parallelism", 32)
		val cfgTaskTimeoutInterval = appConfig.getLong("task-timeout-interval", 30000)

		val cfgCheckpointEnabled = appConfig.getBoolean("checkpoint.enabled", true)
		val cfgCheckpointLocation = appConfig.get("checkpoint.path", "s3://assn-csa-prod-telemetry-data-lake/TestData/Spike/Huyen/SDC/ckpoint/")
		val cfgCheckpointInterval = appConfig.getLong("checkpoint.interval", 800000L)
		val cfgCheckpointTimeout = appConfig.getLong("checkpoint.timeout", 600000L)
		val cfgCheckpointMinPause = appConfig.getLong("checkpoint.minimum-pause", 600000L)

		val cfgSdcHistoricalLocation = appConfig.get("sources.sdc-historical.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/")
		val cfgSdcHistoricalScanInterval = appConfig.getLong("sources.sdc-historical.scan-interval", 10000L)
		val cfgSdcHistoricalScanConsistency = appConfig.getLong("sources.sdc-historical.scan-consistency-offset", 2000L)
		val cfgSdcHistoricalIgnore = appConfig.getLong("sources.sdc-historical.ignore-files-older-than-minutes", 10000) * 60 * 1000

		val cfgSdcInstantLocation = appConfig.get("sources.sdc-instant.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/instant")
		val cfgSdcInstantScanInterval = appConfig.getLong("sources.sdc-instant.scan-interval", 10000L)
		val cfgSdcInstantScanConsistency = appConfig.getLong("sources.sdc-instant.scan-consistency-offset", 2000L)
		val cfgSdcInstantIgnore = appConfig.getLong("sources.sdc-instant.ignore-files-older-than-minutes", 10000) * 60 * 1000

		val cfgFlsParquetLocation = appConfig.get("sources.fls-parquet.path", "s3://thor-pr-data-warehouse-common/common.ipact.fls.fls7_avc_inv/version=0/")
		val cfgFlsParquetScanInterval = appConfig.getLong("sources.fls-parquet.scan-interval", 60000L)
		val cfgFlsParquetScanConsistency = appConfig.getLong("sources.fls-parquet.scan-consistency-offset", 0L)
		val cfgFlsParquetIgnore = appConfig.getLong("sources.fls-parquet.ignore-files-older-than-minutes", 10000) * 60 * 1000

		val cfgAmsParquetLocation = appConfig.get("sources.ams-parquet.path", "s3://thor-pr-data-warehouse-fttx/fttx.ams.inventory.xdsl_port/version=0/")
		val cfgAmsParquetScanInterval = appConfig.getLong("sources.ams-parquet.scan-interval", 60000L)
		val cfgAmsParquetScanConsistency = appConfig.getLong("sources.ams-parquet.scan-consistency-offset", 0L)
		val cfgAmsParquetIgnore = appConfig.getLong("sources.ams-parquet.ignore-files-older-than-minutes", 10000) * 60 * 1000

		val cfgFlsInitialLocation = appConfig.get("sources.fls-initial.path", "file:///Users/Huyen/Desktop/SDC/enrich/")
		val cfgFlsIncrementalLocation = appConfig.get("sources.fls-incremental.path", "file:///Users/Huyen/Desktop/SDCTest/enrich/")

		val cfgElasticSearchEndpoint = appConfig.get("sink.elasticsearch.endpoint", "vpc-assn-dev-chronos-devops-pmwehr2e7xujnadk53jsx4bjne.ap-southeast-2.es.amazonaws.com")
		val cfgElasticSearchBulkInterval = appConfig.getInt("sink.elasticsearch.bulk-interval-ms", 60000)
		val cfgElasticSearchBulkSize = appConfig.getInt("sink.elasticsearch.bulk-size-mb", 10)
		val cfgElasticSearchRetries = appConfig.getInt("sink.elasticsearch.retries-on-failure", 8)

		val cfgElasticSearchCombinedSdcEnabled = appConfig.getBoolean("sink.elasticsearch.sdc.enabled", false)
		val cfgElasticSearchCombinedSdcIndexName = appConfig.get("sink.elasticsearch.sdc.index-name", "copper-sdc-combined-default")
		val cfgElasticSearchCombinedSdcParallelism = appConfig.getInt("sink.elasticsearch.sdc.parallelism", 4)

		val cfgElasticSearchAverageEnabled = appConfig.getBoolean("sink.elasticsearch.average.enabled", false)
		val cfgElasticSearchAverageIndexName = appConfig.get("sink.elasticsearch.average.index-name", "copper-sdc-combined-default")
		val cfgElasticSearchAverageParallelism = appConfig.getInt("sink.elasticsearch.average.parallelism", 4)

		val cfgElasticSearchEnrichmentEnabled = appConfig.getBoolean("sink.elasticsearch.enrichment.enabled", false)
		val cfgElasticSearchEnrichmentIndexName = appConfig.get("sink.elasticsearch.enrichment.index-name", "copper-enrichment-default")
		val cfgElasticSearchEnrichmentParallelism = appConfig.getInt("sink.elasticsearch.enrichment.parallelism", 1)

		val cfgElasticSearchUnenrichableEnabled = appConfig.getBoolean("sink.elasticsearch.unenrichable.enabled", false)
		val cfgElasticSearchUnenrichableIndexName = appConfig.get("sink.elasticsearch.unenrichable.index-name", "copper-sdc-unenrichable-default")
		val cfgElasticSearchUnenrichableParallelism = appConfig.getInt("sink.elasticsearch.unenrichable.parallelism", 2)

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
		val cfgParquetInstantPath = appConfig.get("sink.parquet.instant.path", "file:///Users/Huyen/Desktop/SDCTest/ouput/instant")
		val cfgParquetHistoricalPath = appConfig.get("sink.parquet.historical.path", "file:///Users/Huyen/Desktop/SDCTest/ouput/historical")

		println("Streaming with the following parameters:")

		/** set up the streaming execution environment */
		val streamEnv =
			if (cfgCheckpointEnabled) StreamExecutionEnvironment.getExecutionEnvironment
				.setStateBackend(new RocksDBStateBackend(cfgCheckpointLocation, true))
				.enableCheckpointing(cfgCheckpointInterval)
			else StreamExecutionEnvironment.getExecutionEnvironment
		if (cfgCheckpointEnabled) {
			streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(cfgCheckpointMinPause)
			streamEnv.getCheckpointConfig.setCheckpointTimeout(cfgCheckpointTimeout)
			streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
			streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
			streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
		}
		streamEnv.getConfig.setTaskCancellationInterval(cfgTaskTimeoutInterval)
		streamEnv.setParallelism(cfgParallelism)
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		/** Read fls, ams, and merge them to create enrichment stream */
		val fifFlsParquet = new FlsRawFileInputFormat(new Path(cfgFlsParquetLocation))
		fifFlsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgFlsParquetIgnore))
		fifFlsParquet.setNestedFileEnumeration(true)
		val streamFlsParquet: DataStream[FlsRaw] = streamEnv
			.readFile(fifFlsParquet, cfgFlsParquetLocation,
				FileProcessingMode.PROCESS_CONTINUOUSLY, cfgFlsParquetScanInterval, cfgFlsParquetScanConsistency)
			.uid(OperatorId.SOURCE_FLS_RAW)
			.name("FLS parquet")

		val fifAmsParquet = new AmsRawFileInputFormat(new Path(cfgAmsParquetLocation))
		fifAmsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgAmsParquetIgnore))
		fifAmsParquet.setNestedFileEnumeration(true)
		val streamAmsParquet: DataStream[AmsRaw] = streamEnv
			.readFile(fifAmsParquet, cfgAmsParquetLocation,
				FileProcessingMode.PROCESS_CONTINUOUSLY, cfgAmsParquetScanInterval, cfgAmsParquetScanConsistency)
			.uid(OperatorId.SOURCE_AMS_RAW)
			.name("AMS parquet")

		val streamEnrichment = streamAmsParquet.connect(streamFlsParquet)
			.keyBy(_.customer_id, _.uni_prid)
			.flatMap(new MergeAmsFls)
			.uid(OperatorId.AMS_FLS_MERGER)
			.name("Merge AMS & FLS parquet")
		/** Read fls initial and incremental stream */
		//		val ifFlsInitialCsv = new TextInputFormat(
		//			new org.apache.flink.core.fs.Path(cfgFlsInitialLocation))
		//		ifFlsInitialCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
		//		ifFlsInitialCsv.setNestedFileEnumeration(true)
		//		val streamFlsInitial: DataStream[String] =
		//			streamEnv.readFile(ifFlsInitialCsv, cfgFlsIncrementalLocation,
		//				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
		//					.uid(OperatorId.SOURCE_FLS_INITIAL)
		//					.name("Initial FLS stream")
		//
		//		val ifFlsIncrementalCsv = new TextInputFormat(
		//			new org.apache.flink.core.fs.Path(cfgFlsIncrementalLocation))
		//		ifFlsIncrementalCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
		//		ifFlsIncrementalCsv.setNestedFileEnumeration(true)
		//		val streamFlsIncremental: DataStream[String] =
		//			streamEnv.readFile(ifFlsIncrementalCsv, cfgFlsInitialLocation,
		//				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
		//					.uid(OperatorId.SOURCE_FLS_INCREMENTAL)
		//					.name("Incremental FLS stream")
		//
		//		val flsRaw = streamFlsIncremental.union(streamFlsInitial)
		//				.flatMap(line => {
		//					val v = line.split(",")
		//					List(FlsRecord(v(4).toLong,v(1),v(2),v(3),v(4)))
		//				})

		val streamEnrichmentAgg = streamEnrichment
		//			.union(flsRaw)

		/** Read SDC streams */
		implicit val iParser: SdcParser[SdcRawInstant] = SdcRawInstant
		implicit val hParser: SdcParser[SdcRawHistorical] = SdcRawHistorical

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
		val streamSdcRawInstant: DataStream[SdcRawInstant] = streamDslamInstant
			.flatMap(new ParseSdcRecord[SdcRawInstant])
			.uid(OperatorId.SDC_PARSER_INSTANT)
			.name("Parse SDC Instant")
			.filter(r => !r.data.if_admin_status.equalsIgnoreCase("down"))
			.uid(OperatorId.SDC_PARSER_INSTANT + "_1")

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
		val streamSdcRawHistorical: DataStream[SdcRawHistorical] = streamDslamHistorical
			.flatMap(new ParseSdcRecord[SdcRawHistorical]())
			.uid(OperatorId.SDC_PARSER_HISTORICAL)
			.name("Parse SDC Historical")

		/** Find missing DSLAM */
		lazy val streamDslamMetadataInstant: DataStream[DslamMetadata] = streamDslamInstant
			.map(r => r.metadata)
			.uid("Metadata_Extract_Instant")
		lazy val streamDslamMissingInstant = streamDslamMetadataInstant
			.keyBy(_.name)
			.window(EventTimeSessionWindows.withGap(Time.minutes(2 * cfgRollingAverageSlideInterval)))
			.allowedLateness(Time.minutes(cfgRollingAverageAllowedLateness))
			.aggregate(new IdentifyMissingDslam.AggIncremental, new IdentifyMissingDslam.AggFinal)
			.uid(OperatorId.STATS_MISSING_DSLAM_INSTANT)
			.name("Identify missing Dslam - Instant")
		//		val streamDslamCountInstant = streamDslamMetadataInstant
		//				.windowAll(TumblingEventTimeWindows.of(Time.minutes(cfgRollingAverageSlideInterval)))
		//				.allowedLateness(Time.minutes(cfgRollingAverageSlideInterval))
		//				.aggregate(new CountDslam.AggIncremental, new CountDslam.AggFinal)
		//				.uid(OperatorId.STATS_COUNT_INSTANT)
		//				.name("Count Dslam - Instant")
		//
		lazy val streamDslamMetadataHistorical: DataStream[DslamMetadata] = streamDslamHistorical
			.map(r => r.metadata)
			.uid("Metadata_Extract_Historical")
		lazy val streamDslamMissingHistorical = streamDslamMetadataHistorical
			.keyBy(_.name)
			.window(EventTimeSessionWindows.withGap(Time.minutes(2 * cfgRollingAverageSlideInterval)))
			.allowedLateness(Time.minutes(cfgRollingAverageAllowedLateness))
			.aggregate(new IdentifyMissingDslam.AggIncremental, new IdentifyMissingDslam.AggFinal)
			.uid(OperatorId.STATS_MISSING_DSLAM_HISTORICAL)
			.name("Identify missing Dslam - Historical")
		//		val streamDslamCountHistorical = streamDslamMetadataHistorical
		//				.windowAll(TumblingEventTimeWindows.of(Time.minutes(cfgRollingAverageSlideInterval)))
		//				.allowedLateness(Time.minutes(cfgRollingAverageSlideInterval))
		//				.aggregate(new CountDslam.AggIncremental, new CountDslam.AggFinal)
		//				.uid(OperatorId.STATS_COUNT_HISTORICAL)
		//				.name("Count Dslam - Historical")
		//
		/** Enrich SDC Streams */
		implicit val typeInfo = createTypeInformation[SdcRecord]
		val outputTagI = OutputTag[SdcRawInstant]("unenrichable_i")
		val outputTagH = OutputTag[SdcRawHistorical]("unenrichable_h")

		val streamEnriched: DataStream[SdcEnrichedBase] =
			streamSdcRawInstant.map(_.asInstanceOf[SdcRawBase]).uid("Huyen1")
				.union(streamSdcRawHistorical.map(_.asInstanceOf[SdcRawBase]).uid("Huyen2"))
				//				.assignTimestampsAndWatermarks(new SdcRecordTimeAssigner[SdcRawBase]).name("Assign Timestamp")
				.keyBy(r => (r.dslam, r.port))
				.connect(streamEnrichmentAgg.keyBy(r => (r.dslam, r.port)))
				.process(new EnrichSdcRecord(outputTagI, outputTagH))
				.uid(OperatorId.SDC_ENRICHER)
				.name("Enrich SDC records")
		//		val streamNotEnrichableInstant = streamEnriched.getSideOutput(outputTagI)
		//		val streamNotEnrichableHistorical = streamEnriched.getSideOutput(outputTagH)

		/** split SDC streams into instant and historical */
		val splits: SplitStream[SdcEnrichedBase] = streamEnriched
			.map(a => a).split(_ match {
			case _:SdcEnrichedInstant => List("instant")
			case _:SdcEnrichedHistorical => List("historical")
		})

		val streamEnrichedInstant = splits.select("instant").map(_ match {
			case i: SdcEnrichedInstant => i
		}).uid("Split1")
		val streamEnrichedHistorical = splits.select("historical").map(_ match {
			case h: SdcEnrichedHistorical => h
		}).uid("Split2")

		/** calculate rolling average from enrichment */
		// todo: This section is using an experiment feature to avoid shuffle
		lazy val streamAverage: DataStream[SdcAverage] =
		new DataStreamUtils(streamEnrichedInstant)
			.reinterpretAsKeyedStream(r => (r.dslam, r.port))
			.window(SlidingEventTimeWindows.of(Time.minutes(cfgRollingAverageWindowInterval),
				Time.minutes(cfgRollingAverageSlideInterval)))
			.allowedLateness(Time.minutes(cfgRollingAverageAllowedLateness))
			.trigger(new AverageSdcInstant.AggTrigger(Time.minutes(cfgRollingAverageSlideInterval).toMilliseconds))
			.aggregate(new AverageSdcInstant.AggIncremental, new AverageSdcInstant.AggFinal)
			.uid(OperatorId.SDC_AVERAGER)
			.name("Calculate Rolling Average")

		/** create combined SDC stream */
		//		val combined = streamEnrichedInstant.connect(streamEnrichedHistorical)
		//				.keyBy(r => (r.dslam, r.port, r.ts), r => (r.dslam, r.port, r.ts))
		//	        	.process(new SdcRecordCombiner(10000L, 5000L, 30000L))

		//		val streamCombined = streamEnrichedInstant.connect(streamEnrichedHistorical)
		//				.keyBy(r => (r.dslam, r.port), r => (r.dslam, r.port))
		//				.process(new SdcRecordCombiner2(20000L, 5000L, 30000L))

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
			streamSdcRawHistorical.print()
			//			streamSdcRawInstant.print()
			//			streamEnrichment.print()
			streamEnrichedInstant.print()
			//			streamEnrichment.map(_.toString).print()
			//			streamEnriched.print()
			//			streamNotEnrichable.print()
			//			if (cfgElasticSearchAverageEnabled) streamAverage.filter(s => s.enrich.avcId.equals("AVC000066198558")).print()
			//			streamSdcRawInstant.map(_.toString).addSink(SdcParquetFileSink.buildSink("file:///Users/Huyen/Desktop/SDCTest/output"))
			//			streamSdcRawInstant.addSink(SdcParquetFileSink.buildSink[SdcRawInstant](cfgParquetInstantPath, cfgParquetPrefix, cfgParquetSuffixFormat))
			//			streamEnriched
			//					.addSink(SdcElasticSearchSink.createSink[SdcEnriched](cfgElasticSearchEndpoint,
			//						cfgElasticSearchCombinedSdcIndexName,
			//						cfgElasticSearchBulkInterval,
			//						cfgElasticSearchRetries))
			//					.uid(OperatorId.SINK_ELASTIC_COMBINED)
			//			streamEnrichedInstant.print()
			//			averageInstant.print()
			//			combined.print()
			//			combined.map(r=>r.toString).addSink(ElasticSearch.createSink())
			//			sdcInErr.print()
		} else {
			if (cfgElasticSearchCombinedSdcEnabled)
				streamEnriched
					.addSink(SdcElasticSearchSink.createSdcSink[SdcEnrichedBase](
						cfgElasticSearchEndpoint,
						cfgElasticSearchCombinedSdcIndexName,
						cfgElasticSearchBulkSize,
						cfgElasticSearchBulkInterval,
						cfgElasticSearchRetries,
						SdcElasticSearchSink.UPDATABLE_TIME_SERIES_INDEX))
					.setParallelism(cfgElasticSearchCombinedSdcParallelism)
					.uid(OperatorId.SINK_ELASTIC_COMBINED)
					.name("ES - Combined")

			if (cfgElasticSearchEnrichmentEnabled)
				streamEnrichmentAgg
					.addSink(SdcElasticSearchSink.createSdcSink[FlsRecord](
						cfgElasticSearchEndpoint,
						cfgElasticSearchEnrichmentIndexName,
						-1,
						cfgElasticSearchBulkInterval,
						cfgElasticSearchRetries,
						SdcElasticSearchSink.SINGLE_INSTANCE_INDEX))
					.setParallelism(cfgElasticSearchEnrichmentParallelism)
					.uid(OperatorId.SINK_ELASTIC_ENRICHMENT)
					.name("ES - Enrichment")

			//			if (cfgElasticSearchUnenrichableEnabled)
			//				streamNotEnrichableHistorical
			//					.addSink(SdcElasticSearchSink.createSdcSink[SdcRawHistorical](
			//						cfgElasticSearchEndpoint,
			//						cfgElasticSearchUnenrichableIndexName,
			//						cfgElasticSearchBulkInterval,
			//						cfgElasticSearchRetries,
			//						SdcElasticSearchSink.DAILY_INDEX))
			//					.setParallelism(cfgElasticSearchUnenrichableParallelism)
			//					.uid(OperatorId.SINK_ELASTIC_UNENRICHABLE)
			//					.name("ES - Unenrichable")

			if (cfgElasticSearchAverageEnabled)
				streamAverage
					.addSink(SdcElasticSearchSink.createSdcSink[SdcAverage](
						cfgElasticSearchEndpoint,
						cfgElasticSearchAverageIndexName,
						-1,
						cfgElasticSearchBulkInterval,
						cfgElasticSearchRetries,
						SdcElasticSearchSink.TIME_SERIES_INDEX))
					.setParallelism(cfgElasticSearchAverageParallelism)
					.uid(OperatorId.SINK_ELASTIC_AVERAGE)
					.name("ES - Rolling Average")

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
			}

			if (cfgElasticSearchStatsEnabled) {
				streamDslamMetadataInstant.union(streamDslamMetadataHistorical)
					.addSink(SdcElasticSearchSink.createSingleIndexSink[DslamMetadata](
						cfgElasticSearchEndpoint, cfgElasticSearchStatsDslamMetaIndexName,
						DslamMetadata.toMap,
						r => s"${if (r.isInstant) "I" else "H"}_${r.name}_${r.metricsTime}",
						cfgElasticSearchBulkInterval, cfgElasticSearchRetries))
					.setParallelism(1)
					.uid(OperatorId.SINK_ELASTIC_METADATA)
					.name("ES - DSLAM Info")

				streamDslamMissingInstant
					.addSink(SdcElasticSearchSink.createSingleIndexSink[(Long, String)](
						cfgElasticSearchEndpoint, cfgElasticSearchStatsMissingInstantIndexName,
						r => Map("metrics_timestamp" -> (r._1 + cfgRollingAverageSlideInterval * 60L * 1000L), "dslam" -> r._2),
						r => s"I_${r._2}_${r._1}",
						cfgElasticSearchBulkInterval, cfgElasticSearchRetries))
					.setParallelism(1)
					.uid(OperatorId.SINK_ELASTIC_MISSING_I)
					.name("ES - Missing DSLAM - Instant")

				streamDslamMissingHistorical
					.addSink(SdcElasticSearchSink.createSingleIndexSink[(Long, String)](
						cfgElasticSearchEndpoint, cfgElasticSearchStatsMissingHistoricalIndexName,
						r => Map("metrics_timestamp" -> (r._1 + cfgRollingAverageSlideInterval * 60L * 1000L), "dslam" -> r._2),
						r => s"H_${r._2}_${r._1}",
						cfgElasticSearchBulkInterval, cfgElasticSearchRetries))
					.setParallelism(1)
					.uid(OperatorId.SINK_ELASTIC_MISSING_H)
					.name("ES - Missing DSLAM - Historical")

				//				streamDslamCountInstant.addSink(SdcElasticSearchSink.createSingleIndexSink[(Long, Int)](
				//					cfgElasticSearchEndpoint, "copper-sdc-count-instant",
				//					r => Map("metrics_timestamp" -> r._1, "measurements_count" -> r._2),
				//					cfgElasticSearchBulkInterval, cfgElasticSearchRetries)
				//				).setParallelism(1).name("ES - DSLAM count - Instant")
				//
				//				streamDslamCountHistorical.addSink(SdcElasticSearchSink.createSingleIndexSink[(Long, Int)](
				//					cfgElasticSearchEndpoint, "copper-sdc-count-historical",
				//					r => Map("metrics_timestamp" -> r._1, "measurements_count" -> r._2),
				//					cfgElasticSearchBulkInterval, cfgElasticSearchRetries)
				//				).setParallelism(1).name("ES - DSLAM count - Historical")
				//
			}

			//			averageInstant.addSink(new SdcKinesisProducer[SdcOut](kinesis_stream))
			//			val test2 = averageInstant.filter(s => s.measurements_count == 1)
			//			test2.addSink(new SdcKinesisProducer[SdcOut](kinesis_log_stream))
			//			streamEnrichedInstant.addSink(new SdcKinesisProducer[SdcEnrichedInstant](kinesis_stream))
			//			averageInstant.addSink(ElasticSearch.createSink[SdcOut]())
			//			streamEnrichedInstant.filter(_.port.endsWith("10")).addSink(new SdcKinesisProducer[SdcEnrichedInstant](kinesis_stream))
			//			streamEnrichedHistorical.addSink(new SdcKinesisProducer[SdcEnrichedHistorical](kinesis_stream))
			//			sdcInErr.addSink(kinesis_log)
			//			combined.addSink(new SdcKinesisProducer[SdcCombined](kinesis_stream))
			//			averageInstant.map(r => r.toString)
			//					.addSink(new BucketingSink[String](kinesis_stream))
		}
		// execute program
		streamEnv.execute("Chronos SDC")
	}
}
