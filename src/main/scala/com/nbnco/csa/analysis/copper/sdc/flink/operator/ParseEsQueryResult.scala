//package com.nbnco.csa.analysis.copper.sdc.flink.operator
//
//import com.nbnco.csa.analysis.copper.sdc.data._
//import com.nbnco.csa.analysis.copper.sdc.flink.operator.ParseEsQueryResult.LOG
//import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName._
//
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.util.{Collector, OutputTag}
//import org.elasticsearch.action.search.SearchResponse
//import org.elasticsearch.rest.RestStatus
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.JavaConversions._
//import scala.util.{Success, Try}
//
///**
//  * Created by Huyen on 15/4/19.
//  */
//
//object ParseEsQueryResult {
//	val LOG: Logger = LoggerFactory.getLogger(classOf[ParseEsQueryResult])
//}
//
///**
//  *
//  * This function parses the response from ElasticSearch (SearchResponse). Its output will be three DataStreams:
//  * 	1. One main stream is of type SdcCompact
//  * 	2. One stream of type ((dslam: String, port: String), flag: Boolean), to be collected via the output tag `noSyncCandidatePhysicalRef`
//  * 	3. One stream of type (avc: String, flag: Boolean), to be collected via the output tag `toRetryCandidateLogicalRef`
//  * @param noSyncCandidatePhysicalRef
//  * @param toRetryCandidateLogicalRef
//  * @todo	Delay the output of `toRetryCandidateLogicalRef` via timers
//  */
//class ParseEsQueryResult(noSyncCandidatePhysicalRef: OutputTag[((String, String), JBool)],
//						 toRetryCandidateLogicalRef: OutputTag[(String, JBool)])
//		extends ProcessFunction[(String, JBool, Option[SearchResponse]), SdcCompact] {
//
//	override def processElement(raw: (String, JBool, Option[SearchResponse]), context: ProcessFunction[(String, JBool, Option[SearchResponse]), SdcCompact]#Context, collector: Collector[SdcCompact]): Unit = {
//		raw._3 match {
//			case Some(searchResponse) =>
//				if (searchResponse.status == RestStatus.OK && searchResponse.getHits.totalHits > 0) {
//					/** Parse the results into SdcCompact */
//					searchResponse.getHits.getHits.foreach(r =>
//						Try(SdcCompact(r.getSortValues.head.asInstanceOf[Long], raw._1, r.getSourceAsMap)) match {
//							case Success(sdcCompact) => collector.collect(sdcCompact)
//							case _ => /** to handle issues with parsing records from ES here */
//						}
//					)
//					/** Send out the details to help turning the monitoring flag on/off */
//					val latestRecord = searchResponse.getHits.getAt(0).getSourceAsMap
//					context.output(noSyncCandidatePhysicalRef, ((latestRecord.get(DSLAM).asInstanceOf[String], latestRecord.get(PORT).asInstanceOf[String]), raw._2))
//				} else {
//					if (searchResponse.status == RestStatus.OK)
//						LOG.warn(s"Got zero records from ElasticSearch for AVC ${raw._1}")
//					else
//						LOG.warn(s"Got ${searchResponse.status} while reading data from ElasticSearch for AVC ${raw._1}")
//					triggerRetry(raw._1, raw._2, context)
//				}
//			case None => /** to handle the case when there was error with reading data from ES here */
//				triggerRetry(raw._1, raw._2, context)
//		}
//	}
//
//	//todo: Enhance this
//	/** This is the procedure to send back the AVC that we failed to collect any data from ES, needs to work with Timers
//	  * With this simple implementation, just do nothing */
//	def triggerRetry(avc: String, flag: JBool, context: ProcessFunction[(String, JBool, Option[SearchResponse]), SdcCompact]#Context): Unit = {
//		LOG.warn(s"    Enabling/Disabling Nosync-monitoring for ${avc} failed. Please retry manually")
//		context.output(toRetryCandidateLogicalRef, (avc, flag))
//	}
//}