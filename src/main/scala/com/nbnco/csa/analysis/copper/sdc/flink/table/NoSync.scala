package com.nbnco.csa.analysis.copper.sdc.flink.table

import java.sql.Timestamp

import com.nbnco.csa.analysis.copper.sdc.data.{DslamCompact, SdcCompact}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._

object NoSync {
	def apply(streamEnv: StreamExecutionEnvironment,
			  streamInput: DataStream[SdcCompact],
			  numberOfReInitMeasurements: Int,
			  numberOfOperUpMeasurements: Int,
			  maxReInitCount: Int) = {

//	 			GROUP BY HOP(rowtime, INTERVAL '15' MINUTE, INTERVAL '45' MINUTE
		val queryWithWindows =
			"""SELECT * FROM
	  			(SELECT
	  				ts,
					avc,
					(COUNT(ts) OVER longWindow) AS cnt,
	  				(SUM(reInit) OVER longWindow) AS sumReInit,
	  				(SUM(uas) over shortWindow) AS sumUas
	 			FROM SDC
				WHERE avc IS NOT NULL
	 			WINDOW
	 				longWindow AS (
						PARTITION BY dslam, port
						ORDER BY ts
						ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
					),
	 				shortWindow AS (
						PARTITION BY dslam, port
						ORDER BY ts
						ROWS 2 PRECEDING
	  				)
	   			) WHERE cnt = 3 AND sumReInit <= 6 AND sumUas <= 180
			""".stripMargin

		val query =
			"""SELECT * FROM
	  			(SELECT
	  				ts,
					avc,
					(COUNT(ts) OVER longWindow) AS cnt,
	  				(SUM(reInit) OVER longWindow) AS sumReInit,
	  				(SUM(uas) over shortWindow) AS sumUas
	 			FROM SDC
				WHERE avc IS NOT NULL
	 			WINDOW
	 				longWindow AS (
						PARTITION BY dslam, port
						ORDER BY ts
						ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
					),
	 				shortWindow AS (
						PARTITION BY dslam, port
						ORDER BY ts
						ROWS 2 PRECEDING
	  				)
	   			) WHERE cnt = 3 AND sumReInit <= 60 AND sumUas <= 18000
			""".stripMargin

		val tableEnv = StreamTableEnvironment.create(streamEnv)
		tableEnv.registerDataStream("SDC", streamInput, 'ts.rowtime, 'dslam, 'port, 'avc, 'lpr, 'reInit, 'uas, 'attndrDS, 'attndrUS, 'ifOperStatus)
		val ret = tableEnv.toRetractStream[(Timestamp, String, Long, Short, Short )](tableEnv.sqlQuery(query))
		ret
	}
}


/***
  * Output is calculated along with the advance of watermark.
  */