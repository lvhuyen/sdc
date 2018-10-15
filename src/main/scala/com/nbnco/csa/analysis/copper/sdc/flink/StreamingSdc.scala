//package com.nbnco.csa.analysis.copper.sdc.flink
//
//import java.util.Properties
//
//import com.nbnco.csa.analysis.copper.sdc.data.{FlsRecord, SdcEnrichedInstant, SdcOut, SdcRawInstant}
//import com.nbnco.csa.analysis.copper.sdc.flink.operator.SdcTimeAssigner
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.io.FilePathFilter
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.java.io.TextInputFormat
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
////import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
//import org.apache.flink.streaming.api.functions.source.FileProcessingMode
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
//import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisProducer, KinesisPartitioner}
//import org.apache.flink.streaming.util.serialization.SerializationSchema
//
///**
//  * Skeleton for a Flink Streaming Job.
//  *
//  * To package your appliation into a JAR file for execution, run
//  * 'mvn clean package' on the command line.
//  *
//  * If you change the name of the main class (with the public static void main(String[] args))
//  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
//  */
//object StreamingSdc {
//	def main(args: Array[String]) {
//		val debug = false
//		val parallel: Int = args.lift(0).getOrElse(if (debug) 1 else 8).toString.toInt
//		val path_sdc = args.lift(1).getOrElse(
//			if (debug) "file:///Users/Huyen/Desktop/SDCTest/sdc/"
//			else "s3://assn-huyen/Test/sdc/").toString
//		val path_enrich0 = args.lift(2).getOrElse(
//			if (debug) "file:///Users/Huyen/Desktop/SDC/enrich/"
//			else "s3://assn-huyen/SDC/enrich_full_csv/").toString
//		val path_enrich1 = args.lift(3).getOrElse(
//			if (debug) "file:///Users/Huyen/Desktop/SDCTest/enrich/"
//			else "s3://assn-huyen/Test/enrichment/").toString
//		val kinesis_stream = args.lift(4).getOrElse("assn-huyen").toString
//		val kinesis_log_stream = args.lift(5).getOrElse("assn-huyen-log").toString
//
//		println("Streaming with the following parameters:")
//		println(s"  SDC data: $path_sdc")
//		println(s"  enrich_0: $path_enrich0")
//		println(s"  enrich 1: $path_enrich1")
//		println(s"  kinesis: $kinesis_stream")
//		println(s"  parallelism: $parallel")
//
//		// set up the streaming execution environment
//		val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//	        	.setStateBackend(new FsStateBackend("s3://assn-huyen/flink_ckpoint/"))
//	        	.enableCheckpointing(1000)
//		streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//		streamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
//		streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//		streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//		streamEnv.setParallelism(parallel)
//		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
////		val inputSchema = createTypeInformation[
////				(String, String,
////						Long,
////						String, String,
////						Int, Int,
////						Int, Int,
////						Int,
////						String)]
////		val inputformat = new RowCsvInputFormat(new org.apache.flink.core.fs.Path("file:///Users/Huyen/Desktop/SDCTest/"), Array(inputSchema))
//
////		import collection.JavaConverters._
////		val inputSchema = new RowTypeInfo (
//////			Seq(
////				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
////				BasicTypeInfo.LONG_TYPE_INFO,
////				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
////				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
////				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
////				BasicTypeInfo.INT_TYPE_INFO,
////				BasicTypeInfo.STRING_TYPE_INFO
//////			),
//////			Seq(
//////				"dslam", "port",
//////				"data_time",
//////				"if_admin_status", "if_oper_status",
//////				"actual_ds", "actual_us",
//////				"attndr_ds", "attndr_us",
//////				"attenuation_ds",
//////				"user_mac_address")
////		)
//
//		val sdc_input_format = new TextInputFormat(
//			new org.apache.flink.core.fs.Path(path_sdc))
//		sdc_input_format.setFilesFilter(FilePathFilter.createDefaultFilter())
//		sdc_input_format.setNestedFileEnumeration(true)
//
//		val fls_input_format = new TextInputFormat(
//			new org.apache.flink.core.fs.Path(path_enrich0))
//		fls_input_format.setFilesFilter(FilePathFilter.createDefaultFilter())
//		fls_input_format.setNestedFileEnumeration(true)
//
//		val fls_input_format1 = new TextInputFormat(
//			new org.apache.flink.core.fs.Path(path_enrich1))
//		fls_input_format1.setFilesFilter(FilePathFilter.createDefaultFilter())
//		fls_input_format1.setNestedFileEnumeration(true)
//
//		val outputTag = OutputTag[String]("malformat-output")
//
//		val sdcInRaw: DataStream[SdcRawInstant] = streamEnv
//				.readFile(sdc_input_format,
//					path_sdc,
//					FileProcessingMode.PROCESS_CONTINUOUSLY,
//					500L, 2000L)
//				.process(new ProcessFunction[String, SdcRawInstant] {
//					override def processElement(i: String, ctx: ProcessFunction[String, SdcRawInstant]#Context, collector: Collector[SdcRawInstant]) = {
//						val v = i.split(",")
//						try {
//							if (v.length >= 11)
//								collector.collect(SdcRawInstant(v(0), v(1).toLong, v(2), v(3), v(4), v(5).toInt, v(6).toInt, v(7).toInt, v(8).toInt, v(9), Some(v(10)), System.currentTimeMillis()))
//							else
//								ctx.output(outputTag, "incorrectLength|" + i)
//						}
//						catch {
//							//	todo: log file format error here
//							case s: Throwable => ctx.output(outputTag, s"${s.getMessage}|$i")
//						}
//					}
//				})
//		val sdcInErr = sdcInRaw.getSideOutput(outputTag)
////				.flatMap(line => {
////			        val v = line.split(",")
////					try {
////						if (v.length >= 11)
////							List(SdcIn(v(0), v(1).toLong, v(2), v(3), v(4), v(5).toInt, v(6).toInt, v(7).toInt, v(8).toInt, Some(v(9).toInt), Some(v(10)), System.currentTimeMillis()))
////						else None
////					}
////					catch {
//////						todo: log file format error here
////						case _ => None
////					}
////		        })
////		if (output_at == 1) {
////			println ("HELLO")
////			sdcInRaw.map(r => r.toString)
////					.addSink(new BucketingSink[String](kinesis_stream))
////		}
////		if (output_at == 11) {
////			println ("CHAO")
////			sdcInRaw.print()
////		}
//
////		val postgres = JDBCInputFormat.buildJDBCInputFormat
////	        	.setDrivername("Noname")
////	        	.setDBUrl("labs-orders360-postgres.ctd5i6eczxgx.ap-southeast-2.rds.amazonaws.com")
////	        	.setUsername("orders360_prodsupport")
////	        	.setPassword("orders360_prodsupport")
//
////		streamEnv.createInput()
//
//		val fls2: DataStream[String] =
//			streamEnv.readFile(fls_input_format1,
//				path_enrich1,
//				FileProcessingMode.PROCESS_CONTINUOUSLY,
//				5000L
//			)
//
//		val fls1: DataStream[String] =
//			streamEnv.readFile(fls_input_format,
//				path_enrich0,
//				FileProcessingMode.PROCESS_ONCE,
//				5000L
//			)
//		val flsRaw = fls2.union(fls1)
//			            .flatMap(line => {
//					        val v = line.split(",")
//					        List(FlsRecord(v(0),v(1),v(2),v(3),v(4).toLong))
//				        })
////		if (output_at == 21) fls1.print()
////		if (output_at == 22) fls2.print()
////		if (output_at == 2) {
////			println ("HELLO CUCKOO")
////			flsRaw.map(r => r.toString)
////					.addSink(new BucketingSink[String](kinesis_stream))
////		}
//
//		val merge: DataStream[SdcEnrichedInstant] = sdcInRaw.connect(flsRaw)
//	        	.keyBy(r => (r.dslam, r.port), r2 => (r2.dslam, r2.port))
//				.flatMap(new EnrichSdcRecord)
////		if (output_at == 3) merge.print()
//
//		val sdcInAverage: DataStream[SdcOut] = merge
//				.assignTimestampsAndWatermarks(new SdcTimeAssigner[SdcEnrichedInstant])
//	        	.keyBy(r => (r.dslam, r.port))
//				.window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(15)))
//				.allowedLateness(Time.minutes(16))
//				.trigger(new SdcTrigger(Time.minutes(15).toMilliseconds))
//				.aggregate(new AverageAgg, new WindowAverageAgg)
////		if (output_at == 0) sdcInAverage.print()
//
////		if (output_at == 88)
////			sdcInAverage.writeAsText(kinesis_stream, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
////		if (output_at == 89)
////			sdcInAverage.map(r => r.toString)
////					.addSink(new BucketingSink[String](kinesis_stream))
//
//		val producerConfig = new Properties
//		// Required configs
//		producerConfig.put(AWSConfigConstants.AWS_REGION, "ap-southeast-2")
//		producerConfig.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO")
//		// Optional configs
//		producerConfig.put("AggregationMaxCount", "4294967295")
//		producerConfig.put("CollectionMaxCount", "100") // some number between 1 and 500
//		producerConfig.put("RecordTtl", "30000")
//		producerConfig.put("RequestTimeout", "6000")
//		producerConfig.put("ThreadPoolSize", "15")
//		producerConfig.put("RateLimit", "100")
//
//		val kinesis = new FlinkKinesisProducer[SdcOut](
//			new SerializationSchema[SdcOut] {
//				def serialize(element: SdcOut) = element.toString.getBytes("UTF-8")
//			},
//			producerConfig)
//		kinesis.setCustomPartitioner(
//			new KinesisPartitioner[SdcOut] {
//				def getPartitionId(element: SdcOut) = element.dslam})
//
//		kinesis.setFailOnError(false)
//		kinesis.setDefaultStream(kinesis_stream)
//		kinesis.setDefaultPartition("0")
//
//		val kinesis_log = new FlinkKinesisProducer[String](
//			new SerializationSchema[String] {
//				override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")
//			}, producerConfig)
//		kinesis_log.setFailOnError(false)
//		kinesis_log.setDefaultStream(kinesis_log_stream)
//		kinesis_log.setDefaultPartition("0")
//
//		//		if (output_at == 99){
////			sdcInAverage.addSink(kinesis)
////		}
//		if (debug) {
//				sdcInAverage.print()
//				sdcInErr.print()
//			}
//		else {
//			sdcInAverage.addSink(kinesis)
//			sdcInErr.addSink(kinesis_log)
////			sdcInAverage.map(r => r.toString)
////					.addSink(new BucketingSink[String](kinesis_stream))
//		}
//
//		// execute program
//		streamEnv.execute("HUYEN's JOB")
//	}
//
//	/** User-defined WindowFunction to compute the average temperature of SensorReadings */
//	class TemperatureAverager extends WindowFunction[SdcRawInstant, SdcOut, (String, String), TimeWindow] {
//
//		/** apply() is invoked once for each window */
//		override def apply(
//				                  lineId: (String, String),
//				                  window: TimeWindow,
//				                  vals: Iterable[SdcRawInstant],
//				                  out: Collector[SdcOut]): Unit = {
//
//			// compute the average temperature
//			val (cnt, sum_ds, sum_us) = vals.foldLeft((0, 0.0, 0.0))((c, r) => (c._1 + 1, c._2 + r.attndr_ds, c._3 + r.attndr_us))
//			val avg_ds = sum_ds / cnt
//			val avg_us = sum_us / cnt
//
//			val latest_rec = vals.last
//
//			// emit a SensorReading with the average temperature
//			out.collect(SdcOut(
//				lineId._1, lineId._2,
//				"AVC", "CPI",
//				window.getEnd, window.getEnd,
//				latest_rec.attndr_ds, latest_rec.attndr_us,
//				cnt,
//				avg_ds.toInt, avg_us.toInt))
//		}
//	}
//
//	class AverageAgg0 extends AggregateFunction[SdcEnrichedInstant, (Int, Int, Int, SdcEnrichedInstant), SdcOut] {
//		override def createAccumulator() = (0, 0, 0, SdcEnrichedInstant(0,0, "","","","","","",0,0,0,0,"",None,0))
//
//		override def add(in: SdcEnrichedInstant, acc: (Int, Int, Int, SdcEnrichedInstant)) =
//			(acc._1 + in.attndr_ds,
//					acc._2 + in.attndr_us,
//					acc._3 + 1,
//					if (acc._4.chronos_time > in.chronos_time) acc._4 else in
//			)
//
//		override def merge(acc: (Int, Int, Int, SdcEnrichedInstant), acc1: (Int, Int, Int, SdcEnrichedInstant)) =
//			(acc._1 + acc1._1,
//					acc._2 + acc1._2,
//					acc._3 + acc1._3,
//					if (acc._4.chronos_time > acc1._4.chronos_time) acc._4 else acc1._4)
//
//		override def getResult(acc: (Int, Int, Int, SdcEnrichedInstant)) =
//			SdcOut(acc._4, acc._1 / acc._3, acc._2 / acc._3, acc._3)
//	}
//
//	class AverageAgg extends AggregateFunction[SdcEnrichedInstant, (Int, Int, Int), (Int, Int, Int)] {
//		override def createAccumulator() = (0, 0, 0)
//
//		override def add(in: SdcEnrichedInstant, acc: (Int, Int, Int)) =
//			(acc._1 + in.attndr_ds,
//					acc._2 + in.attndr_us,
//					acc._3 + 1)
//
//		override def merge(acc: (Int, Int, Int), acc1: (Int, Int, Int)) =
//			(acc._1 + acc1._1, acc._2 + acc1._2, acc._3 + acc1._3)
//
//		override def getResult(acc: (Int, Int, Int)) = {
//			(acc._1 / acc._3, acc._2 / acc._3, acc._3)}
//	}
//
//	class WindowAverageAgg extends ProcessWindowFunction[(Int, Int, Int), SdcOut, (String, String), TimeWindow] {
//
//		def process(key: (String, String), context: Context, averages: Iterable[(Int, Int, Int)], out: Collector[SdcOut]) = {
//			val aver = averages.iterator.next()
////			context.windowState.
//			val curEvent = context.windowState.getState(new ValueStateDescriptor[SdcEnrichedInstant]("curEvent", createTypeInformation[SdcEnrichedInstant])).value()
//			out.collect(SdcOut(curEvent, aver._1, aver._2, aver._3))
//		}
//	}
//
//	class SdcTrigger(slidingInterval: Long) extends Trigger[SdcEnrichedInstant, TimeWindow] {
//		override def onElement(t: SdcEnrichedInstant, l: Long, w: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
//			val curEvent: ValueState[SdcEnrichedInstant] = ctx.getPartitionedState(
//				new ValueStateDescriptor[SdcEnrichedInstant]("curEvent", createTypeInformation[SdcEnrichedInstant]))
//
//			val triggered: ValueState[Boolean] = ctx.getPartitionedState(
//				new ValueStateDescriptor[Boolean]("triggered", createTypeInformation[Boolean]))
//
//			curEvent.update(t)
//
//			if (triggered.value())
//				TriggerResult.FIRE
//			else if (l >= w.getEnd - slidingInterval) {
//				triggered.update(true)
//				TriggerResult.FIRE
//			}
//			else TriggerResult.CONTINUE
//		}
//
//		override def onEventTime(l: Long, w: TimeWindow, ctx: Trigger.TriggerContext) = {
//			TriggerResult.PURGE
//		}
//
//		override def clear(w: TimeWindow, ctx: Trigger.TriggerContext) = {
//			ctx.getPartitionedState(
//				new ValueStateDescriptor[SdcEnrichedInstant]("curEvent", createTypeInformation[SdcEnrichedInstant])).clear()
//			ctx.getPartitionedState(
//				new ValueStateDescriptor[Boolean]("triggered", createTypeInformation[Boolean])).clear()
//		}
//
//		override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext) = TriggerResult.CONTINUE
//	}
//
//	class EnrichSdcRecord extends RichCoFlatMapFunction[SdcRawInstant, FlsRecord, SdcEnrichedInstant] {
//		val mappingDescriptor = new ValueStateDescriptor[(String, String, Long)]("mapping", classOf[(String, String, Long)])
//		mappingDescriptor.setQueryable("Enrichment")
//
//		override def flatMap1(in1: SdcRawInstant, out: Collector[SdcEnrichedInstant]): Unit = {
//			val mapping = getRuntimeContext.getState(mappingDescriptor).value
//			val (avc_id, cpi, enrichment_date) =
//				if (mapping != null) (mapping._1, mapping._2, mapping._3)
//				else ("AVC", "CPI", 0L)
//			out.collect(SdcEnrichedInstant(in1, avc_id, cpi, enrichment_date))
//		}
//
//		override def flatMap2(in2: FlsRecord, out: Collector[SdcEnrichedInstant]): Unit = {
//			val mapping = getRuntimeContext.getState(mappingDescriptor)
//			if (mapping.value == null || mapping.value._3 < in2.metrics_date)
//					mapping.update((in2.avc_id, in2.cpi, in2.metrics_date))
//		}
//	}
//}
