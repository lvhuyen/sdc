package com.nbnco.csa.analysis.copper.sdc.flink

import com.nbnco.csa.analysis.copper.sdc.data.{DslamRaw, EnrichmentRecord, SdcRecord}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Huyen on 5/10/18.
  */
package object operator {
	class SdcRecordTimeAssigner[Type <: SdcRecord]
			extends BoundedOutOfOrdernessTimestampExtractor[Type](Time.seconds(0)) {
		override def extractTimestamp(r: Type): Long = r.ts
	}

	class DslamRecordTimeAssigner[T]
			extends BoundedOutOfOrdernessTimestampExtractor[DslamRaw[T]](Time.seconds(0)) {
		override def extractTimestamp(r: DslamRaw[T]): Long = r.metadata.ts
	}
}
