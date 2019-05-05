package com.nbnco.csa.analysis.copper.sdc.flink.sink

import java.util.Properties

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisProducer, KinesisPartitioner}
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}

/**
  * Created by Huyen on 24/8/18.
  */
class SdcKinesisProducer[T] private (serializationSchema: SerializationSchema[T], config: Properties)
		extends FlinkKinesisProducer[T](serializationSchema, config) {
	def this(defaultStreamName: String) = {
		this(new SerializationSchema[T] {
				def serialize(element: T): Array[Byte] = element.toString.getBytes("UTF-8")
			},
			{
				val kinesisProducerConfig = new Properties
				// Required configs
				kinesisProducerConfig.put(AWSConfigConstants.AWS_REGION, "ap-southeast-2")
				kinesisProducerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO")
				// Optional configs
				kinesisProducerConfig.put("AggregationMaxCount", "4294967295")
				kinesisProducerConfig.put("CollectionMaxCount", "100") // some number between 1 and 500
				kinesisProducerConfig.put("RecordTtl", "30000")
				kinesisProducerConfig.put("RequestTimeout", "6000")
				kinesisProducerConfig.put("ThreadPoolSize", "15")
				kinesisProducerConfig.put("RateLimit", "100")
				kinesisProducerConfig
			}
		)
		this.setDefaultStream(defaultStreamName)
		this.setDefaultPartition("UNKNOWN_DSLAM")
		this.setFailOnError(false)
		this.setCustomPartitioner(new KinesisPartitioner[T] {
			override def getPartitionId(t: T): String = t.toString
		})
	}
}

object SdcKinesisProducer {
	def apply[T](streamName: String): FlinkKinesisProducer[T] = {
		val producerConfig = new Properties()
		// Required configs
		producerConfig.put(AWSConfigConstants.AWS_REGION, "ap-southeast-2")
		producerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO")
		//		producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
		//		producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
		// Optional KPL configs
		producerConfig.put("AggregationMaxCount", "4294967295")
		producerConfig.put("CollectionMaxCount", "100")
		producerConfig.put("RecordTtl", "30000")
		producerConfig.put("RequestTimeout", "6000")
		producerConfig.put("ThreadPoolSize", "15")
		producerConfig.put("RateLimit", "100")

		// Disable Aggregation if it's not supported by a consumer
		// producerConfig.put("AggregationEnabled", "false")
		// Switch KinesisProducer's threading model
		// producerConfig.put("ThreadingModel", "PER_REQUEST")

		val kinesis = new FlinkKinesisProducer[T]((t: T) => t.toString.getBytes, producerConfig)
		kinesis.setFailOnError(false)
		kinesis.setDefaultStream(streamName)
		kinesis.setDefaultPartition("0")

		kinesis
	}

	def apply[T <: AnyRef](streamName: String, partitioner: T => String): FlinkKinesisProducer[T] = {
		val kinesis = apply[T](streamName)
		kinesis.setCustomPartitioner(partitioner(_))
		kinesis
	}
}