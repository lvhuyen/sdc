package com.nbnco.csa.analysis.copper.sdc.flink.sink

import java.util.Properties

import com.nbnco.csa.analysis.copper.sdc.data.SdcRecord
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisProducer, KinesisPartitioner}
import org.apache.flink.streaming.util.serialization.SerializationSchema

/**
  * Created by Huyen on 24/8/18.
  */
class SdcKinesisProducer[T <: SdcRecord] private (serializationSchema: SerializationSchema[T], config: Properties)
		extends FlinkKinesisProducer[T](serializationSchema, config) {
	def this(defaultStreamName: String) = {
		this(
			new SerializationSchema[T] {
				def serialize(element: T): Array[Byte] = element.toString.getBytes("UTF-8")
			}, {
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
			override def getPartitionId(t: T): String = t.dslam
		})
	}
}
