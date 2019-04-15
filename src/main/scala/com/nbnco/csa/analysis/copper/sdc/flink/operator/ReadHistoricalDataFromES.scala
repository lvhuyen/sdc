package com.nbnco.csa.analysis.copper.sdc.flink.operator

import java.util.concurrent.TimeUnit

import com.nbnco.csa.analysis.copper.sdc.data.SdcCompact
import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.FieldName._
import com.nbnco.csa.analysis.copper.sdc.flink.operator.ReadHistoricalDataFromES.{ConnectTimeout, LOG, SocketTimeout}
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
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

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}


object ReadHistoricalDataFromES {
	val LOG: Logger = LoggerFactory.getLogger(classOf[ReadHistoricalDataFromES])
	val ConnectTimeout: Int = 10000
	val SocketTimeout: Int = 5000

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
}

class ReadHistoricalDataFromES(serverUrl: String, indexName: String, docType: String, recordsCount: Int, searchTimeout: Int)
		extends AsyncFunction[(String, Boolean), (String, Boolean, Option[SearchResponse])] {

	/** Private members */
	private lazy val esClient = new RestHighLevelClient(
		RestClient.builder(new HttpHost(serverUrl, 443, "https"))
				.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
					override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
						requestConfigBuilder
								.setConnectTimeout(ConnectTimeout)
								.setSocketTimeout(SocketTimeout)
				}))

	private lazy val searchRequest: SearchRequest = new SearchRequest(indexName).types(docType)

	private lazy val searchSourceBuilderFull: SearchSourceBuilder = new SearchSourceBuilder()
			.timeout(new TimeValue(searchTimeout, TimeUnit.SECONDS))
			.sort(new FieldSortBuilder(TS).order(SortOrder.DESC))
			.fetchSource(Array(IFOPERSTATUS, MAC, DSLAM, PORT, LPR, REINIT, UAS, ATTNDRDS, ATTNDRUS), Array[String]())
			.size(recordsCount)

	private lazy val searchSourceBuilderLite: SearchSourceBuilder = new SearchSourceBuilder()
			.timeout(new TimeValue(searchTimeout, TimeUnit.SECONDS))
			.sort(new FieldSortBuilder(TS).order(SortOrder.DESC))
			.fetchSource(Array(DSLAM, PORT), Array[String]())
			.size(1)

	private lazy val directExecCtx = ExecutionContext.fromExecutor(
		org.apache.flink.runtime.concurrent.Executors.directExecutor())

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
//				resultFuture.completeExceptionally(e)
		}(directExecCtx)
	}
}
//
//class ReadHistoricalDataFromES2(serverUrl: String, indexName: String, docType: String, recordsCount: Int, searchTimeout: Int)
//		extends AsyncFunction[String, Array[SdcCompact]] {
//
//	/** Private members */
//	private lazy val includedFields = Array(IFOPERSTATUS, MAC, DSLAM, PORT, LPR, REINIT, UAS, ATTNDRDS, ATTNDRUS)
//
//	private lazy val esClient = new RestHighLevelClient(
//		RestClient.builder(new HttpHost(serverUrl, 443, "https"))
//				.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
//					override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
//						requestConfigBuilder
//								.setConnectTimeout(ConnectTimeout)
//								.setSocketTimeout(SocketTimeout)
//				}))
//	private lazy val searchRequest: SearchRequest = new SearchRequest(indexName).types(docType)
//	private lazy val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
//			.size(recordsCount)
//			.timeout(new TimeValue(searchTimeout, TimeUnit.SECONDS))
//			.sort(new FieldSortBuilder(TS).order(SortOrder.DESC))
//			.fetchSource(includedFields, Array[String]())
//	private lazy val directExecCtx = ExecutionContext.fromExecutor(
//		org.apache.flink.runtime.concurrent.Executors.directExecutor())
//
//	/** An ActionListener class that implement an Async call-back from ES client */
//	private class ESQueryListener extends ActionListener[SearchResponse] {
//		private val promise = Promise[SearchResponse]()
//		def onResponse(response: SearchResponse) { promise.success(response) }
//		def onFailure(e: Exception) { promise.failure(e) }
//
//		def result: Future[SearchResponse] = promise.future
//	}
//
//
//	/** The context used for the future callbacks */
//	implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
//
//	override def asyncInvoke(avc: String, resultFuture: ResultFuture[Array[SdcCompact]]): Unit = {
//		val listener = new ESQueryListener
//
//		// issue the asynchronous request, receive a future for the result
//		val req = searchRequest.source(searchSourceBuilder.query(QueryBuilders.termQuery(AVC, avc)))
//		esClient.searchAsync(req, listener)
//
//		val resultFutureRequested: Future[Array[SdcCompact]] = listener.result.map { searchResponse =>
//			if (searchResponse.status == RestStatus.OK) {
//				for {
//					r <- searchResponse.getHits.getHits()
//					b = Try(SdcCompact(r.getSortValues.head.asInstanceOf[Long], avc, r.getSourceAsMap))
//					if b.isSuccess
//				} yield b.get
//			} else {
//				LOG.warn(s"Got ${searchResponse.status} while reading data from ElasticSearch for AVC ${searchRequest.source.query}")
//				Array.empty
//			}
//		}
//
//		// set the callback to be executed once the request by the client is complete
//		// the callback simply forwards the result to the result future
//		resultFutureRequested.onComplete {
//					case Success(r) =>
//						resultFuture.complete(Iterable(r))
//					case Failure(e) => {
//						LOG.warn(s"Error while reading data from ElasticSearch for AVC ${searchRequest.source.query}: ${e}")
//						resultFuture.complete(Iterable(Array.empty))
////						resultFuture.completeExceptionally(e)
//					}
//		}(directExecCtx)
//	}
//}
