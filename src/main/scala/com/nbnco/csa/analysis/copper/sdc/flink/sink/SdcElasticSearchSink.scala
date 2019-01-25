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
					.upsert(data)
		}
		override def process(element: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
			requestIndexer.add(createUpdateRequest(element))
		}
	}

	private object SdcElasticSearchFailureHandler extends ActionRequestFailureHandler {
		@throws[Throwable]
		override def onFailure(actionRequest: ActionRequest, failure: Throwable, restStatusCode: Int, indexer: RequestIndexer): Unit = {
			if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]) != Optional.empty() ||
						ExceptionUtils.findThrowable(failure, classOf[ElasticsearchParseException]) != Optional.empty()) {
				// full queue or malformed document
				LOG.warn("Failed inserting record to ElasticSearch: statusCode {} message: {} record: {} stacktrace {}.\nRetrying",
					restStatusCode.toString, failure.getMessage, actionRequest.toString, failure.getStackTrace)
				actionRequest match {
					case s: UpdateRequest => indexer.add(s)
					case s: IndexRequest => indexer.add(s)
					case _ =>
				}
			} else if (ExceptionUtils.findThrowable(failure, classOf[org.elasticsearch.index.engine.VersionConflictEngineException]) != Optional.empty()) {
				LOG.warn("Failed inserting record to ElasticSearch: statusCode {} message: {} record: {} stacktrace {}.\nRetrying",
					restStatusCode.toString, failure.getMessage, actionRequest.toString, failure.getStackTrace)
				actionRequest match {
					case s: UpdateRequest => indexer.add(s)
					case s: IndexRequest => indexer.add(s)
					case _ =>
				}
			} else {
				LOG.error(s"ELASTICSEARCH FAILED:\n    statusCode $restStatusCode\n    message: ${failure.getMessage}\n${failure.getStackTrace}")
				val inner = failure.getCause
				if (inner != null)
					LOG.error(s"    INNER:\n    message: ${inner.getMessage}\n${inner.getStackTrace}")
				LOG.error(s"    DATA:\n    ${actionRequest.toString}")
			}
		}
	}

	def createUpdatableSdcElasticSearchSink[T <: SdcRecord](endpoint: String, indexNamePrefix: String, bulkSize: Int, bulkInterval: Int, flushRetries: Int) = {
		val httpHosts = new java.util.ArrayList[HttpHost]
		httpHosts.add(new HttpHost(endpoint, 443, "https"))

		val esSinkBuilder = new ElasticsearchSink.Builder[T](
			httpHosts,
			new UpsertDataSinkFunction[T](
				r => s"$indexNamePrefix-${DAILY_INDEX_SUFFIX_FORMATTER.format(Instant.ofEpochMilli(r.ts))}",
				r => s"${r.dslam}_${r.port}_${r.ts}"
			)
		)

		esSinkBuilder.setFailureHandler(SdcElasticSearchFailureHandler)
		if (bulkSize > 0)
			esSinkBuilder.setBulkFlushMaxSizeMb(bulkSize)
		if (bulkInterval > 0)
			esSinkBuilder.setBulkFlushInterval(bulkInterval)
		esSinkBuilder.setBulkFlushBackoff(true)
		esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL)
		esSinkBuilder.setBulkFlushBackoffDelay(50)
		esSinkBuilder.setBulkFlushBackoffRetries(flushRetries)

		esSinkBuilder.build()
	}

	def createElasticSearchSink[T](endpoint: String, indexNameBuilder: T => String,
																									dataBuilder: T => Map[String, Any], docIdBuilder: T => String,
																									bulkInterval: Int, flushRetries: Int) = {
		val httpHosts = new java.util.ArrayList[HttpHost]
		httpHosts.add(new HttpHost(endpoint, 443, "https"))

		val esSinkBuilder = new ElasticsearchSink.Builder[T](
			httpHosts,
			new InsertDataSinkFunction(indexNameBuilder,
				dataBuilder,
				docIdBuilder
			)
		)

		esSinkBuilder.setFailureHandler(SdcElasticSearchFailureHandler)
		esSinkBuilder.setBulkFlushMaxSizeMb(10)
		if (bulkInterval > 0)
			esSinkBuilder.setBulkFlushInterval(bulkInterval)
		esSinkBuilder.setBulkFlushBackoff(true)
		esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL)
		esSinkBuilder.setBulkFlushBackoffDelay(50)
		esSinkBuilder.setBulkFlushBackoffRetries(flushRetries)

		esSinkBuilder.build()
	}
}
