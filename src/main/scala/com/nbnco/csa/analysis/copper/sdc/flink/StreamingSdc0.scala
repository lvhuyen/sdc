//package com.nbnco.csa.analysis.copper.sdc.flink
//
//import com.nbnco.csa.analysis.copper.sdc.data._
//import com.nbnco.csa.analysis.copper.sdc.flink.operator.{AmsFlsMerger, EnrichSdcRecord, SdcTimeAssigner}
//import com.nbnco.csa.analysis.copper.sdc.flink.sink.SdcElasticSearchSink
//import com.nbnco.csa.analysis.copper.sdc.flink.source._
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.functions.source.FileProcessingMode
//import org.apache.flink.streaming.api.scala.{DataStream, _}
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
////import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
////import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisProducer, KinesisPartitioner}
//
//object StreamingSdc0 {
//	object OperatorId {
//		val SOURCE_SDC_INSTANT = "Source_Sdc_Instant"
//		val SOURCE_SDC_HISTORICAL = "Source_Sdc_Historical"
//		val SOURCE_FLS_INITIAL = "Source_Fls_Initial"
//		val SOURCE_FLS_INCREMENTAL = "Source_Fls_Incremental"
//		val SOURCE_FLS_RAW = "Source_Fls_Raw"
//		val SOURCE_AMS_RAW = "Source_Ams_Raw"
//		val SINK_ELASTIC_COMBINED = "Elastic_Combined"
//		val SINK_ELASTIC_AVERAGE = "Elastic_Average"
//		val SINK_ELASTIC_UNENRICHABLE = "Elastic_Unenrichable"
//		val SDC_ENRICHER = "Sdc_Enricher"
//		val SDC_AVERAGER = "Sdc_Averager"
//		val SDC_COMBINER = "Sdc_Combiner"
//		val AMS_FLS_MERGER = "Ams_Fls_Merger"
//	}
//
//	def main(args: Array[String]) {
//
//		/** Read configuration */
//		val configFile = ParameterTool.fromArgs(args).get("configFile", "dev.properties")
//		val appConfig = ParameterTool.fromPropertiesFile(configFile)
//		val parallelism = appConfig.getInt("parallelism", 16)
//		val debug = appConfig.getBoolean("debug", false)
//
//		val cfgSdcHistoricalLocation = appConfig.get("sources.sdc-historical.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/")
//		val cfgSdcHistoricalScanInterval = appConfig.getLong("sources.sdc-historical.scan-interval", 10000L)
//		val cfgSdcHistoricalScanConsistency = appConfig.getLong("sources.sdc-historical.scan-consistency-offset", 2000L)
//		val cfgSdcHistoricalIgnore = appConfig.getLong("sources.sdc-historical.ignore-files-older-than-minutes", 10000) * 60 * 1000
//
//		val cfgSdcInstantLocation = appConfig.get("sources.sdc-instant.path", "s3://thor-pr-data-raw-fttx/fttx.sdc.copper.history.combined/instant")
//		val cfgSdcInstantScanInterval = appConfig.getLong("sources.sdc-instant.scan-interval", 10000L)
//		val cfgSdcInstantScanConsistency = appConfig.getLong("sources.sdc-instant.scan-consistency-offset", 2000L)
//		val cfgSdcInstantIgnore = appConfig.getLong("sources.sdc-instant.ignore-files-older-than-minutes", 10000) * 60 * 1000
//
//		val cfgFlsParquetLocation = appConfig.get("sources.fls-parquet.path", "s3://thor-pr-data-warehouse-common/common.ipact.fls.fls7_avc_inv/version=0/")
//		val cfgFlsParquetScanInterval = appConfig.getLong("sources.fls-parquet.scan-interval", 60000L)
//		val cfgFlsParquetScanConsistency = appConfig.getLong("sources.fls-parquet.scan-consistency-offset", 0L)
//		val cfgFlsParquetIgnore = appConfig.getLong("sources.fls-parquet.ignore-files-older-than-minutes", 10000) * 60 * 1000
//
//		val cfgAmsParquetLocation = appConfig.get("sources.ams-parquet.path", "s3://thor-pr-data-warehouse-fttx/fttx.ams.inventory.xdsl_port/version=0/")
//		val cfgAmsParquetScanInterval = appConfig.getLong("sources.ams-parquet.scan-interval", 60000L)
//		val cfgAmsParquetScanConsistency = appConfig.getLong("sources.ams-parquet.scan-consistency-offset", 0L)
//		val cfgAmsParquetIgnore = appConfig.getLong("sources.ams-parquet.ignore-files-older-than-minutes", 10000) * 60 * 1000
//
//		val cfgFlsInitialLocation = appConfig.get("sources.fls-initial.path", "file:///Users/Huyen/Desktop/SDC/enrich/")
//		val cfgFlsIncrementalLocation = appConfig.get("sources.fls-incremental.path", "file:///Users/Huyen/Desktop/SDCTest/enrich/")
//
//		val cfgCheckpointEnabled = appConfig.getBoolean("checkpoint.enabled", true)
//		val cfgCheckpointLocation = appConfig.get("checkpoint.path", "s3://assn-csa-prod-telemetry-data-lake/TestData/Spike/Huyen/SDC/ckpoint/")
//		val cfgCheckpointInterval = appConfig.getLong("checkpoint.interval", 800000L)
//		val cfgCheckpointTimeout = appConfig.getLong("checkpoint.timeout", 600000L)
//		val cfgCheckpointMinPause = appConfig.getLong("checkpoint.minimum-pause", 600000L)
//
//		val cfgElasticSearchEndpoint = appConfig.get("sink.elasticsearch.endpoint", "vpc-assn-dev-chronos-devops-pmwehr2e7xujnadk53jsx4bjne.ap-southeast-2.es.amazonaws.com")
//		val cfgElasticSearchFlushInterval = appConfig.getInt("sink.elasticsearch.bulk-interval", 60000)
//		val cfgElasticSearchRetries = appConfig.getInt("sink.elasticsearch.retries-on-failure", 8)
//		val cfgElasticSearchIndexName = appConfig.get("sink.elasticsearch.sdc.index-name", "copper-sdc-combined-default")
//		val cfgElasticSearchUnenrichableIndexName = appConfig.get("sink.elasticsearch.unenrichable.index-name", "copper-sdc-unenrichable-default")
//		val cfgElasticSearchParallelism = appConfig.getInt("sink.elasticsearch.sdc.parallelism", 6)
//		val cfgElasticSearchUnenrichableParallelism = appConfig.getInt("sink.elasticsearch.unenrichable.parallelism", 2)
//
//		println("Streaming with the following parameters:")
//
//		/** set up the streaming execution environment */
//		val streamEnv =
//			if (cfgCheckpointEnabled) StreamExecutionEnvironment.getExecutionEnvironment
//					.setStateBackend(new RocksDBStateBackend(cfgCheckpointLocation))
//					.enableCheckpointing(cfgCheckpointInterval)
//			else StreamExecutionEnvironment.getExecutionEnvironment
//		if (cfgCheckpointEnabled) {
//			streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(cfgCheckpointMinPause)
//			streamEnv.getCheckpointConfig.setCheckpointTimeout(cfgCheckpointTimeout)
//			streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//			streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//			streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
//		}
//
//		streamEnv.setParallelism(parallelism)
//		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//		/** Read fls, ams, and merge them to create enrichment stream */
//		val fifFlsParquet = new FlsRawFileInputFormat(new Path(cfgFlsParquetLocation))
//		fifFlsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgFlsParquetIgnore))
//		fifFlsParquet.setNestedFileEnumeration(true)
//		val streamFlsParquet: DataStream[FlsRaw] = streamEnv
//				.readFile(fifFlsParquet, cfgFlsParquetLocation,
//					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgFlsParquetScanInterval, cfgFlsParquetScanConsistency)
//	        	.uid(OperatorId.SOURCE_FLS_RAW)
//	        	.name("Reader: FLS raw")
//
//		val fifAmsParquet = new AmsRawFileInputFormat(new Path(cfgAmsParquetLocation))
//		fifAmsParquet.setFilesFilter(new EnrichmentFilePathFilter(cfgAmsParquetIgnore))
//		fifAmsParquet.setNestedFileEnumeration(true)
//		val streamAmsParquet: DataStream[AmsRaw] = streamEnv
//				.readFile(fifAmsParquet, cfgAmsParquetLocation,
//					FileProcessingMode.PROCESS_CONTINUOUSLY, cfgAmsParquetScanInterval, cfgAmsParquetScanConsistency)
//	        	.uid(OperatorId.SOURCE_AMS_RAW)
//				.name("Reader: AMS raw")
//
//		val streamEnrichment = streamAmsParquet.connect(streamFlsParquet)
//        		.keyBy(_.customer_id, _.uni_prid)
//        		.flatMap(new AmsFlsMerger)
//				.uid(OperatorId.AMS_FLS_MERGER)
//	        	.name("Merge AMS & FLS")
//
//		/** Read fls initial and incremental stream */
////		val ifFlsInitialCsv = new TextInputFormat(
////			new org.apache.flink.core.fs.Path(cfgFlsInitialLocation))
////		ifFlsInitialCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
////		ifFlsInitialCsv.setNestedFileEnumeration(true)
////		val streamFlsInitial: DataStream[String] =
////			streamEnv.readFile(ifFlsInitialCsv, cfgFlsIncrementalLocation,
////				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
////
////		val ifFlsIncrementalCsv = new TextInputFormat(
////			new org.apache.flink.core.fs.Path(cfgFlsIncrementalLocation))
////		ifFlsIncrementalCsv.setFilesFilter(FilePathFilter.createDefaultFilter())
////		ifFlsIncrementalCsv.setNestedFileEnumeration(true)
////		val streamFlsIncremental: DataStream[String] =
////			streamEnv.readFile(ifFlsIncrementalCsv, cfgFlsInitialLocation,
////				FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
////
////		val flsRaw = streamFlsIncremental.union(streamFlsInitial)
////				.flatMap(line => {
////					val v = line.split(",")
////					List(FlsRecord(v(0),v(1),v(2),v(3),v(4).toLong))
////				})
//
//		/** Read SDC streams */
//		val pathSdcHistorical = new org.apache.flink.core.fs.Path(cfgSdcHistoricalLocation)
//		val fifSdcHistorical = new Dslam1HistoricalInputFormat(pathSdcHistorical)
//		fifSdcHistorical.setFilesFilter(new SdcFilePathFilter(cfgSdcHistoricalIgnore))
//		fifSdcHistorical.setNestedFileEnumeration(true)
//		val streamSdcHistoricalRaw: DataStream[SdcRaw] = streamEnv
//				.readFile(fifSdcHistorical,
//					cfgSdcHistoricalLocation,
//					FileProcessingMode.PROCESS_CONTINUOUSLY,
//					cfgSdcHistoricalScanInterval, cfgSdcHistoricalScanConsistency)
//	        	.uid(OperatorId.SOURCE_SDC_HISTORICAL)
//	        	.name("Reader: Sdc Historical")
//
//		val pathSdcInstant = new org.apache.flink.core.fs.Path(cfgSdcInstantLocation)
//		val fifSdcInstant = new Dslam1InstantInputFormat(pathSdcInstant)
//		fifSdcInstant.setFilesFilter(new SdcFilePathFilter(cfgSdcInstantIgnore))
//		fifSdcInstant.setNestedFileEnumeration(true)
//		val streamSdcInstantRaw: DataStream[SdcRaw] = streamEnv
//				.readFile(fifSdcInstant,
//					cfgSdcInstantLocation,
//					FileProcessingMode.PROCESS_CONTINUOUSLY,
//					cfgSdcInstantScanInterval, cfgSdcInstantScanConsistency)
//				.uid(OperatorId.SOURCE_SDC_INSTANT)
//				.name("Reader: Sdc Instant")
//	        	.filter(x => x match {
//			        case r: SdcRawInstant => !r.data.if_admin_status.equalsIgnoreCase("down")
//			        case _ => true
//		        })
//
//		/** Enrich SDC Streams */
//		implicit val typeInfo = createTypeInformation[SdcRecord]
//		val outputTag = OutputTag[SdcRecord]("unenrichable")
//
//		val streamEnriched: DataStream[SdcEnriched] = streamSdcInstantRaw.union(streamSdcHistoricalRaw)
//				.keyBy(r => (r.dslam, r.port))
//				.connect(streamEnrichment.keyBy(r => (r.dslam, r.port)))
//				.process(new EnrichSdcRecord(outputTag))
//				.uid(OperatorId.SDC_ENRICHER)
//	        	.name("Enrich SDC records")
//		val streamNotEnrichable = streamEnriched.getSideOutput(outputTag)
//
//		/** split SDC streams into instant and historical */
////		val splits: SplitStream[SdcEnriched] = streamEnriched
////				.split(_ match {
////					case _:SdcEnrichedInstant => List("instant")
////					case _:SdcEnrichedHistorical => List("historical")
////				})
////
////		val enrichedInstant = splits.select("instant").map(_ match {
////			case i: SdcEnrichedInstant => i
////		})
////		val enrichedHistorical = splits.select("historical").map(_ match {
////			case h: SdcEnrichedHistorical => h
////		})
//
//		/** calculate rolling average from enrichment */
////		val averageInstant: DataStream[SdcOut] = enrichedInstant
////				.assignTimestampsAndWatermarks(new SdcTimeAssigner[SdcEnrichedInstant]).name("Assign Timestamp")
////	        	.keyBy(r => (r.dslam, r.port))
////				.window(SlidingEventTimeWindows.of(Time.minutes(120), Time.minutes(15)))
////				.allowedLateness(Time.minutes(600))
////				.trigger(new SdcInstantAveragerTrigger(Time.minutes(15).toMilliseconds))
////				.aggregate(new SdcInstantAveragerAggIncremental, new SdcInstantAveragerAggFinal)
////
//		/** create combined SDC stream */
////		val combined = enrichedInstant.connect(enrichedHistorical)
////				.keyBy(r => (r.dslam, r.port, r.ts), r => (r.dslam, r.port, r.ts))
////	        	.process(new CombineSdcRecord(10000L, 5000L, 30000L))
//
////		val combined = enrichedInstant.connect(enrichedHistorical)
////				.keyBy(r => (r.dslam, r.port), r => (r.dslam, r.port))
////				.process(new CombineSdcRecord2(20000L, 5000L, 30000L))
//
//		if (debug) {
//			streamSdcHistoricalRaw.print()
//			streamSdcInstantRaw.print()
////			streamEnrichment.print()
//			streamEnriched.print()
//			streamNotEnrichable.print()
////			streamSdcHistoricalRaw.print()
////			streamSdcInstantRaw.print()
////			streamEnriched
////					.addSink(SdcElasticSearchSink.createSink[SdcEnriched](cfgElasticSearchEndpoint,
////						cfgElasticSearchIndexName,
////						cfgElasticSearchFlushInterval,
////						cfgElasticSearchRetries))
////					.uid(OperatorId.SINK_ELASTIC_COMBINED)
////			enrichedInstant.print()
////			averageInstant.print()
////			combined.print()
////			combined.map(r=>r.toString).addSink(ElasticSearch.createSink())
////			sdcInErr.print()
//		} else {
//			streamEnriched
//					.addSink(SdcElasticSearchSink.createSink[SdcEnriched](
//						cfgElasticSearchEndpoint,
//						cfgElasticSearchIndexName,
//						cfgElasticSearchFlushInterval,
//						cfgElasticSearchRetries))
//					.setParallelism(cfgElasticSearchParallelism)
//					.uid(OperatorId.SINK_ELASTIC_COMBINED)
//		        	.name("ElasticSearch - Combined SDC records")
//
//			streamNotEnrichable
//					.addSink(SdcElasticSearchSink.createSink[SdcRecord](
//						cfgElasticSearchEndpoint,
//						cfgElasticSearchUnenrichableIndexName,
//						cfgElasticSearchFlushInterval,
//						cfgElasticSearchRetries, SdcElasticSearchSink.DAILY_INDEX))
//					.setParallelism(cfgElasticSearchUnenrichableParallelism)
//					.uid(OperatorId.SINK_ELASTIC_UNENRICHABLE)
//					.name("ElasticSearch - Unenrichable SDC records")
//			//			averageInstant.addSink(new SdcKinesisProducer[SdcOut](kinesis_stream))
//			//			val test2 = averageInstant.filter(s => s.measurements_count == 1)
//			//			test2.addSink(new SdcKinesisProducer[SdcOut](kinesis_log_stream))
//			//			enrichedInstant.addSink(new SdcKinesisProducer[SdcEnrichedInstant](kinesis_stream))
//			//			averageInstant.addSink(ElasticSearch.createSink[SdcOut]())
//			//			enrichedInstant.filter(_.port.endsWith("10")).addSink(new SdcKinesisProducer[SdcEnrichedInstant](kinesis_stream))
//			//			enrichedHistorical.addSink(new SdcKinesisProducer[SdcEnrichedHistorical](kinesis_stream))
//			//			sdcInErr.addSink(kinesis_log)
//			//			combined.addSink(new SdcKinesisProducer[SdcCombined](kinesis_stream))
//			//			averageInstant.map(r => r.toString)
//			//					.addSink(new BucketingSink[String](kinesis_stream))
//		}
//		// execute program
//		streamEnv.execute("Chronos SDC")
//	}
//}
