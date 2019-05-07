package com.starfox.analysis.copper.sdc.flink.operator

import java.util.concurrent.TimeUnit

import com.starfox.analysis.copper.sdc.data.SdcCompact
import com.starfox.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName._
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, OutputTag, createTypeInformation}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}


object ReadHistoricalDataFromES {
	private val LOG: Logger = LoggerFactory.getLogger(classOf[ReadHistoricalDataFromES])
	private val EsConnectTimeout: Int = 10000
	private val EsSocketTimeout: Int = 5000
	private val AsyncQueueCapacity: Int = 4
	private val Parallelism: Int = 2

	object FieldName {
		val TS = "metrics_timestamp"
		val DSLAM = "dslam"
		val PORT = "ObjectID"
		val IFOPERSTATUS = "ifOperStatus"
		val AVC = "avcid"
		val MAC = "macaddress"
		val LPR = "xdslFarEndLinePreviousIntervalLPRCounter"
		val REINIT = "xdslLinePreviousIntervalReInitCounter"
		val ATTNDRDS = "xdslFarEndChannelAttainableNetDataRateDownstream"
		val ATTNDRUS = "xdslChannelAttainableNetDataRateUpstream"
		val UAS = "xdslLinePreviousIntervalUASCounter"
	}

	/**
	  * This Flink function reads data from Elasticsearch for those input AVCs.
	  * @param streamInput: a DataStream of (AVC, flag)
	  * @param esServerUrl
	  * @param indexName
	  * @param docType
	  * @param maxNumberOfRecordsToRead
	  * @param esQueryTimeoutInSeconds
	  * @return: a tuple of 3 DataStream:
	  *         DataStream[SdcCompact]: for each input AVC with flag = true, this stream has up to `noOfRecordsToRead` when flag = true
	  *         DataStream[(String, String), Bool)]: this is the mapping of input stream (AVC, flag) -> ((dslam, port), flag)
	  *         DataStream[(String, Bool)]: the records from the input stream which the function failed to read data from ElasticSearch
	  */
	def apply(streamInput: DataStream[(String, Boolean)],
			  esServerUrl: String,
			  indexName: String,
			  docType: String,
			  maxNumberOfRecordsToRead: Int,
			  esQueryTimeoutInSeconds: Int): (DataStream[SdcCompact], DataStream[((String, String), Boolean)], DataStream[(String, Boolean)]) = {

		//  Read Async. Adding 5 seconds to the timeout so this Async function would never timeout
		val streamEsSearchResponse = AsyncDataStream.unorderedWait(streamInput,
			new ReadHistoricalDataFromES(esServerUrl, indexName, docType, maxNumberOfRecordsToRead, esQueryTimeoutInSeconds), esQueryTimeoutInSeconds + 5, TimeUnit.SECONDS, AsyncQueueCapacity)
        		.setParallelism(Parallelism)

		val noSyncCandidatePhysicalRef = OutputTag[((String, String), Boolean)]("NoSyncTogglePhysicalRef")
		val toRetryCandidateLogicalRef = OutputTag[(String, Boolean)]("NoSyncToggleRetry")

		val streamEsData: DataStream[SdcCompact] =
			streamEsSearchResponse.process(new ParseEsQueryResult(noSyncCandidatePhysicalRef, toRetryCandidateLogicalRef))

		(streamEsData.assignTimestampsAndWatermarks(SdcRecordTimeExtractor[SdcCompact]),
				streamEsData.getSideOutput(noSyncCandidatePhysicalRef),
				streamEsData.getSideOutput(toRetryCandidateLogicalRef))
	}

	/**
	  *
	  * This function parses the response from ElasticSearch (SearchResponse). Its output will be three DataStreams:
	  * 	1. One main stream is of type SdcCompact
	  * 	2. One stream of type ((dslam: String, port: String), flag: Boolean), to be collected via the output tag `noSyncCandidatePhysicalRef`
	  * 	3. One stream of type (avc: String, flag: Boolean), to be collected via the output tag `toRetryCandidateLogicalRef`
	  * @param noSyncCandidatePhysicalRef
	  * @param toRetryCandidateLogicalRef
	  * @todo	Delay the output of `toRetryCandidateLogicalRef` via timers
	  */
	private class ParseEsQueryResult(noSyncCandidatePhysicalRef: OutputTag[((String, String), Boolean)],
							 toRetryCandidateLogicalRef: OutputTag[(String, Boolean)])
			extends ProcessFunction[(String, Boolean, Option[SearchResponse]), SdcCompact] {

		override def processElement(raw: (String, Boolean, Option[SearchResponse]), context: ProcessFunction[(String, Boolean, Option[SearchResponse]), SdcCompact]#Context, collector: Collector[SdcCompact]): Unit = {
			raw._3 match {
				case Some(searchResponse) =>
					if (searchResponse.status == RestStatus.OK && searchResponse.getHits.totalHits > 0) {
						if (raw._2) {
							/** Only in case the flag is "on": parse the results into SdcCompact */
							searchResponse.getHits.getHits.reverseIterator.foreach(r =>
								Try(SdcCompact(r.getSortValues.head.asInstanceOf[Long], raw._1, r.getSourceAsMap.asScala)) match {
									case Success(sdcCompact) => collector.collect(sdcCompact)
									case Failure(e) =>
										LOG.warn(s"Error while parsing data from ElasticSearch for AVC ${raw._1} with data ${r.getSourceAsMap}: ${e}")
								}
							)
						}
						/** Send out the details to help turning the monitoring flag on/off */
						val latestRecord = searchResponse.getHits.getAt(0).getSourceAsMap
						context.output(noSyncCandidatePhysicalRef, ((latestRecord.get(DSLAM).asInstanceOf[String], latestRecord.get(PORT).asInstanceOf[String]), raw._2))
					} else {
						if (searchResponse.status == RestStatus.OK)
							LOG.warn(s"Got zero records from ElasticSearch for AVC ${raw._1}")
						else
							LOG.warn(s"Got ${searchResponse.status} while reading data from ElasticSearch for AVC ${raw._1}")
						triggerRetry(raw._1, raw._2, context)
					}
				case None => /** to handle the case when there was error with reading data from ES here */
					triggerRetry(raw._1, raw._2, context)
			}
		}

		//todo: Enhance this
		/** This is the procedure to send back the AVC that we failed to collect any data from ES, needs to work with Timers
		  * With this simple implementation, just do nothing */
		def triggerRetry(avc: String, flag: Boolean, context: ProcessFunction[(String, Boolean, Option[SearchResponse]), SdcCompact]#Context): Unit = {
			LOG.warn(s"    Enabling/Disabling Nosync-monitoring for ${avc} failed. Please retry manually")
			context.output(toRetryCandidateLogicalRef, (avc, flag))
		}
	}


	/**
	  * This Flink function has input data of (AVC, flag). It reads data from Elasticsearch for those input AVCs.
	  * The output stream is of type Option[es.SearchResponse), with up to `noOfRecordsToRead` when flag = true, or up to 1 records when flag = false
	  * @param esServerUrl
	  * @param indexName
	  * @param docType
	  * @param noOfRecordsToRead
	  * @param searchTimeout: timeout (in seconds) to set in the ES search request
	  */
	private class ReadHistoricalDataFromES(esServerUrl: String, indexName: String, docType: String, noOfRecordsToRead: Int, searchTimeout: Int)
			extends AsyncFunction[(String, Boolean), (String, Boolean, Option[SearchResponse])] {

		/** Private members */
		private lazy val esClient = new RestHighLevelClient(
			RestClient.builder(new HttpHost(esServerUrl, 443, "https"))
					.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
						override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
							requestConfigBuilder
									.setConnectTimeout(EsConnectTimeout)
									.setSocketTimeout(EsSocketTimeout)
					}))

		private lazy val searchRequest: SearchRequest = new SearchRequest(indexName).types(docType)

		private lazy val searchSourceBuilderFull: SearchSourceBuilder = new SearchSourceBuilder()
				.timeout(new TimeValue(searchTimeout, TimeUnit.SECONDS))
				.sort(new FieldSortBuilder(TS).order(SortOrder.DESC))
				.fetchSource(Array(IFOPERSTATUS, MAC, DSLAM, PORT, LPR, REINIT, UAS, ATTNDRDS, ATTNDRUS), Array[String]())
				.size(noOfRecordsToRead)

		private lazy val searchSourceBuilderLite: SearchSourceBuilder = new SearchSourceBuilder()
				.timeout(new TimeValue(searchTimeout, TimeUnit.SECONDS))
				.sort(new FieldSortBuilder(TS).order(SortOrder.DESC))
				.fetchSource(Array(DSLAM, PORT), Array[String]())
				.size(1)

		/** The context used for the future callbacks */
		private lazy val directExecCtx = ExecutionContext.fromExecutor(Executors.directExecutor())

		/** An ActionListener class that implement an Async call-back from ES client */
		private class ESQueryListener extends ActionListener[SearchResponse] {
			private val promise = Promise[SearchResponse]()
			def onResponse(response: SearchResponse) { promise.success(response) }
			def onFailure(e: Exception) { promise.failure(e) }

			def result: Future[SearchResponse] = promise.future
		}


		/** The context used for the future callbacks */
		implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

		override def asyncInvoke(input: (String, Boolean),
								 resultFuture: ResultFuture[(String, Boolean, Option[SearchResponse])]): Unit = {
			val listener = new ESQueryListener

			//	Issue the asynchronous request, receive a future for the result
			//	If the input boolean is set, then query the whole needed history
			//	Otherwise, just take the latest
			val req = searchRequest.source((
					if (input._2) searchSourceBuilderFull
					else searchSourceBuilderLite
					).query(QueryBuilders.termQuery(AVC, input._1)))
			esClient.searchAsync(req, listener)

			val resultFutureRequested: Future[SearchResponse] = listener.result

			// set the callback to be executed once the request by the client is complete
			// the callback simply forwards the result to the result future
			resultFutureRequested.onComplete {
				case Success(r) =>
					resultFuture.complete(Iterable((input._1, input._2, Some(r))))
				case Failure(e) =>
					LOG.warn(s"Error while reading data from ElasticSearch for AVC ${searchRequest.source.query}: ${e}")
					resultFuture.complete(Iterable((input._1, input._2, None)))
			}(directExecCtx)
		}
	}
}