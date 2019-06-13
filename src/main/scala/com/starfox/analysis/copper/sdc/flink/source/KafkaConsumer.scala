package com.starfox.analysis.copper.sdc.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaConsumer {
	private val regexRequest = """^(AVC\d{12})(?:[, ](?:(on|true)|(off|false)))?$""".r
	def readNoSyncMonitorRequestFromKafka(streamEnv: StreamExecutionEnvironment, streamName: String): DataStream[(String, Boolean)] = {
		val consumerConfig = new Properties()
		consumerConfig.setProperty("bootstrap.servers", "10.11.37.100:9092")
		// only required for Kafka 0.8
		consumerConfig.setProperty("zookeeper.connect", "10.11.37.100:2181")
		consumerConfig.setProperty("group.id", "Chronos-Flink")

		val flinkConsumer = new FlinkKafkaConsumer[String](streamName, new SimpleStringSchema, consumerConfig)
        				.setStartFromEarliest()

		streamEnv.addSource[String](flinkConsumer)
				.flatMap(_.split("""\r?\n"""))
				.flatMap(_ match {
					case regexRequest(a, b, c) =>
						Seq((a, c == null))
					case _ => Seq.empty
				})
	}
}
