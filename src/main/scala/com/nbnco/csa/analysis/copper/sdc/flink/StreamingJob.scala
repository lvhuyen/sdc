/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nbnco.csa.analysis.copper.sdc.flink

import com.nbnco.csa.analysis.copper.sdc.data.{PojoFls, SdcDataEnrichment, SdcDataInstant, SdcEnrichedInstant}
import com.nbnco.csa.analysis.copper.sdc.flink.StreamingSdcWithAverageByDslam.OperatorId
import com.nbnco.csa.analysis.copper.sdc.flink.sink.SdcParquetFileSink
import com.nbnco.csa.analysis.copper.sdc.flink.source.{EnrichmentFilePathFilter, FlsParquetFileInputFormat}
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
              .setStateBackend(new RocksDBStateBackend("file:///Users/Huyen/Desktop/SDCTest/ckpoint/", true))
              .enableCheckpointing(60000)
    streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    streamEnv.setParallelism(2)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /** Read fls, ams, and merge them to create enrichment stream */
    val path = "file:///Users/Huyen/Desktop/SDCTest/parquet"
    val fifFlsParquet = new TextInputFormat(new Path(path ))
    fifFlsParquet.setFilesFilter(FilePathFilter.createDefaultFilter())
    val stream1: DataStream[String] = streamEnv
            .readFile(fifFlsParquet, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000L, 0L)
            .uid(OperatorId.SOURCE_FLS_RAW)
            .name("FLS parquet")

    val stream2 = stream1.map(r => {
      val v = r.split(',')
      new SdcEnrichedInstant(v(0).toLong, v(1), v(2), SdcDataEnrichment(v(3).toLong, v(4), v(5)),
        new SdcDataInstant(v(6),v(7),v(8).toInt,v(9).toInt,v(10).toInt, v(11).toInt, v(12).toInt, v(13)))
    })

    stream2.print()

    stream2.addSink(
      SdcParquetFileSink.buildSinkReflect[SdcEnrichedInstant]("file:///Users/Huyen/Desktop/SDCTest/parquet_out", "dt=", "yyyy-MM-dd"))
            .setParallelism(2)
            .uid(OperatorId.SINK_PARQUET_INSTANT)
            .name("S3 - Instant")

    // execute program
    streamEnv.execute("Flink Streaming Scala API Skeleton")
  }
}
