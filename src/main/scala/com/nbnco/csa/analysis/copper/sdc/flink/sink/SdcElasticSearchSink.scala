package com.nbnco.csa.analysis.copper.sdc.flink.sink

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Optional

import com.nbnco.csa.analysis.copper.sdc.data.{SdcEnrichedBase, SdcRecord, TemporalEvent}
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkBase, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by Huyen on 19/9/18.
  */

object SdcElasticSearchSink {
	private val INDEX_TYPE = "_doc"
	private val LOG = LoggerFactory.getLogger(SdcElasticSearchSink.getClass)

	val DAILY_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern("uuuuMMdd").withZone(ZoneId.of("UTC"))
	val MONTHLY_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern("uuuuMM").withZone(ZoneId.of("UTC"))

	val UPDATABLE_TIME_SERIES_INDEX = 0
	val SINGLE_INSTANCE_INDEX = 1
	val TIME_SERIES_INDEX = 2
	val DAILY_INDEX = 3
	val MONTHLY_INDEX = 4

	private class InsertDataSinkFunction[T] (indexNameBuilder: (T) => String, dataBuilder: (T) => Map[String, Any], idBuilder: T => String)
			extends ElasticsearchSinkFunction[T] {
		def createIndexRequest(element: T): IndexRequest = {
			new IndexRequest(indexNameBuilder(element), INDEX_TYPE, idBuilder(element))
					.source(mapAsJavaMap(dataBuilder(element)))
		}
		override def process(element: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
			requestIndexer.add(createIndexRequest(element))
		}
	}

	private class UpsertDataSinkFunction[T <: SdcRecord] (indexNameBuilder: (T) => String, idBuilder: T => String)
			extends ElasticsearchSinkFunction[T] {
		def createUpdateRequest(element: T): UpdateRequest = {
			val data = mapAsJavaMap(element.toMap)
			new UpdateRequest(indexNameBuilder(element), INDEX_TYPE, idBuilder(element))
					.doc(data)
					.docAsUpsert(true)
	  			.retryOnConflict(2)
	  			.detectNoop(false)
		}
		override def process(element: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
			requestIndexer.add(createUpdateRequest(element))
		}
	}

	/**
		* This class is to handle failure with ElasticSearch. Currently it handle the problem when connection to ES is
		* interrupted or when there's a version conflict in document.
		* When a retry-able error happened, the request's version will be increased by @versionIncreaseStep.
		* For one request, if there have been more than @furtherRetries, then the exception will be thrown
		*
		* @param furtherRetries: maximum number of retries
		*/
	private class SdcElasticSearchFailureHandler(furtherRetries: Int) extends ActionRequestFailureHandler {
		val versionIncreaseStep: Int = 100
		val versionRetryThreshold: Int = furtherRetries * versionIncreaseStep

		@throws[Throwable]
		private def escalateError(actionRequest: ActionRequest, failure: Throwable, restStatusCode: Int) = {
			LOG.error(s"ELASTICSEARCH FAILED:\n    statusCode $restStatusCode\n    message: ${failure.getMessage}\n${failure.getStackTrace}")
			val inner = failure.getCause
			if (inner != null)
				LOG.error(s"    INNER:\n    message: ${inner.getMessage}\n${inner.getStackTrace}")
			LOG.error(s"    DATA:\n    ${actionRequest.toString}")
			throw failure
		}

		@throws[Throwable]
		override def onFailure(actionRequest: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer): Unit = {
			actionRequest match {
				case s: UpdateRequest =>
					if (s.version() < versionRetryThreshold &&
						(ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]) != Optional.empty()
							|| ExceptionUtils.findThrowableWithMessage(failure, "version_conflict_engine_exception") != Optional.empty()
							|| (restStatusCode == -1 && failure != null & failure.getMessage.contains("Connection closed")))) {
						LOG.info(s"Failed updating document in ElasticSearch with error message '${failure.getMessage}'. Retrying: ${actionRequest.toString}")
						indexer.add(s.version(s.version() + versionIncreaseStep))
					} else
						escalateError(actionRequest, failure, restStatusCode)

				case s: IndexRequest =>
					if (s.version() < versionRetryThreshold &&
						(ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]) != Optional.empty()
							|| (restStatusCode == -1 && failure != null & failure.getMessage.contains("Connection closed")))) {
						LOG.info(s"Failed inserting document to ElasticSearch with error message '${failure.getMessage}'. Retrying: ${actionRequest.toString}")
						indexer.add(s.version(s.version() + versionIncreaseStep))
					} else
						escalateError(actionRequest, failure, restStatusCode)

				case _ => escalateError(actionRequest, failure, restStatusCode)
			}
		}
	}

	def createUpdatableSdcElasticSearchSink[T <: SdcRecord](endpoint: String, indexNamePrefix: String,
																													bulkSize: Int, bulkInterval: Int, flushBackoffDelay: Int,
																													retriesOnError: Int) = {
		val httpHosts = new java.util.ArrayList[HttpHost]
		httpHosts.add(new HttpHost(endpoint, 443, "https"))

		val esSinkBuilder = new ElasticsearchSink.Builder[T](
			httpHosts,
			new UpsertDataSinkFunction[T](
				r => s"$indexNamePrefix-${DAILY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r.ts))}",
				r => s"${r.dslam}_${r.port}_${r.ts}"
			)
		)

		esSinkBuilder.setFailureHandler(new SdcElasticSearchFailureHandler(retriesOnError))
		if (bulkSize > 0)
			esSinkBuilder.setBulkFlushMaxSizeMb(bulkSize)
		if (bulkInterval > 0)
			esSinkBuilder.setBulkFlushInterval(bulkInterval)
		esSinkBuilder.setBulkFlushBackoff(true)
		esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL)
		esSinkBuilder.setBulkFlushBackoffDelay(flushBackoffDelay)

		esSinkBuilder.build()
	}

	def createElasticSearchSink[T](endpoint: String, indexNameBuilder: T => String,
																 dataBuilder: T => Map[String, Any], docIdBuilder: T => String,
																 bulkInterval: Int, flushBackoffDelay: Int,
																 retriesOnError: Int) = {
		val httpHosts = new java.util.ArrayList[HttpHost]
		httpHosts.add(new HttpHost(endpoint, 443, "https"))

		val esSinkBuilder = new ElasticsearchSink.Builder[T](
			httpHosts,
			new InsertDataSinkFunction(indexNameBuilder,
				dataBuilder,
				docIdBuilder
			)
		)

		esSinkBuilder.setFailureHandler(new SdcElasticSearchFailureHandler(retriesOnError))
		esSinkBuilder.setBulkFlushMaxSizeMb(10)
		if (bulkInterval > 0)
			esSinkBuilder.setBulkFlushInterval(bulkInterval)
		esSinkBuilder.setBulkFlushBackoff(true)
		esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL)
		esSinkBuilder.setBulkFlushBackoffDelay(flushBackoffDelay)

		esSinkBuilder.build()
	}
}
