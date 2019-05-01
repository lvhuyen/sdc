package com.nbnco.csa.analysis.copper.sdc.flink

import com.nbnco.csa.analysis.copper.sdc.data.{EnrichmentData, TemporalEvent}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Huyen on 5/10/18.
  */
package object operator {
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
		val SINK_PARQUET_COMBINED = "S3_Parquet_Instant"
		val SINK_PARQUET_HISTORICAL = "S3_Parquet_Historical"
		val SINK_PARQUET_METADATA = "S3_Parquet_Metadata"
		val SINK_PARQUET_UNENRICHABLE_I = "S3_Parquet_Unenrichable_Instant"
		val SINK_PARQUET_UNENRICHABLE_H = "S3_Parquet_Unenrichable_Historical"
		val SDC_ENRICHER = "Sdc_Enricher"
		val SDC_AVERAGER = "Sdc_Averager"
		val SDC_COMBINER = "Sdc_Combiner"
		val STATS_COUNT_HISTORICAL = "Stats_Count_Historical"
		val STATS_COUNT_INSTANT = "Stats_Count_Instant"
		val STATS_MISSING_DSLAM = "Stats_Missing_Dslam"
		val AMS_FLS_MERGER = "Ams_Fls_Merger"
		val SDC_PARSER = "Sdc_Parser"
		val SDC_PARSER_HISTORICAL = "Sdc_Parser_Historical"
		val SDC_PARSER_INSTANT = "Sdc_Parser_Instant"
	}

	object SdcRecordTimeExtractor {
		def apply[Type <: TemporalEvent] = new BoundedOutOfOrdernessTimestampExtractor[Type](Time.milliseconds(0)) {
			override def extractTimestamp(t: Type): Long = t.ts
		}
	}

	object EnrichmentRecordTimeExtractorForIdleSources {
		def apply[Type <: TemporalEvent] = new AssignerWithPeriodicWatermarks[Type] {
			override def getCurrentWatermark: Watermark = Watermark.MAX_WATERMARK
			override def extractTimestamp(t: Type, prevT: Long): Long = t.ts
		}
	}

	object ProcessingTimeExtractorForIdleSources {
		def apply[Type] = new AssignerWithPeriodicWatermarks[Type] {
			override def getCurrentWatermark: Watermark = Watermark.MAX_WATERMARK
			override def extractTimestamp(t: Type, prevT: Long): Long = new java.util.Date().getTime
		}
	}
}
