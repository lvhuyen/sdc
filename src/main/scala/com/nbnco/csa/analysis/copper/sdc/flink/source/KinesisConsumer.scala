package com.nbnco.csa.analysis.copper.sdc.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}

object KinesisConsumer {
	def apply(streamName: String) = {
		val consumerConfig = new Properties()
		consumerConfig.put(AWSConfigConstants.AWS_REGION, "ap-southeast-2")
		consumerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE")
		consumerConfig.put(AWSConfigConstants.AWS_PROFILE_NAME, "saml")
//		consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
//		consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
		consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON")

		new FlinkKinesisConsumer[String](streamName, new SimpleStringSchema, consumerConfig)
	}

	private val r = """^(AVC\d{12})(?:[, ](?:(on|true)|(off|false)))?$""".r
	def readNoSyncMonitorRequestFromKinesis(streamEnv: StreamExecutionEnvironment, kinesisStreamName: String): DataStream[(String, Boolean)] = {
		streamEnv.addSource[String](KinesisConsumer(kinesisStreamName))
				.flatMap(_.split("""\r?\n"""))
				.flatMap(_ match {
					case r(a, b, c) =>
						Seq((a, c == null))
					case _ => Seq.empty
				})
	}
}
