package com.nbnco.csa.analysis.copper.sdc.flink.sink

import java.text.SimpleDateFormat

import com.nbnco.csa.analysis.copper.sdc.data._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

import scala.reflect.ClassTag

/**
  * Created by Huyen on 4/10/18.
  */
object SdcParquetFileSink {
//	def buildSink(outputPath: String, filenameFormat: String = "yyyy-MM-dd", checkInterval: Long = 15): StreamingFileSink[SdcRawInstant] = {
//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(classOf[SdcRawInstant]))
//				.withBucketAssigner(new SdcTimeBucketAssigner[SdcRawInstant]("dt=", filenameFormat))
//			        	.withBucketCheckInterval(checkInterval * 60L * 1000L)
//				.build()
//	}

//	def buildSink[T <: SdcRecord](outputPath: String, prefix: String, suffixFormat: String)(implicit ct: ClassTag[T]): StreamingFileSink[T] = {
//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(ct.runtimeClass)).asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
//				.withBucketCheckInterval(5L * 60L * 1000L)
//				.withBucketAssigner(new DateTimeBucketAssigner[T]("yyyy-MM-dd--HH"))
//				.build()
//	}

	def buildSinkReflect[T <: CopperLine](outputPath: String, prefix: String, suffixFormat: String)(implicit ct: ClassTag[T]): StreamingFileSink[T] = {
		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(ct.runtimeClass)).asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
//				.withBucketCheckInterval(3L * 60L * 1000L)
				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
				.build()
	}

//	def buildSink1[T <: SpecificRecordBase with SdcRecord](outputPath: String, prefix: String, suffixFormat: String)(implicit ct: ClassTag[T]): StreamingFileSink[T] = {
//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forSpecificRecord(ct.runtimeClass)).asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
//				.withBucketCheckInterval(3L * 60L * 1000L)
//				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
//				.build()
//	}

	def buildSinkGeneric[T <: TemporalEvent](schema: Schema, outputPath: String, prefix: String, suffixFormat: String)(implicit ct: ClassTag[T]): StreamingFileSink[T] = {
		val a = ParquetAvroWriters.forGenericRecord(schema)
		StreamingFileSink.forBulkFormat(new Path(outputPath), a).asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
				//				.withBucketCheckInterval(3L * 60L * 1000L)
				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
				.build()
	}
//	def buildSink2(outputPath: String, prefix: String, suffixFormat: String): StreamingFileSink[SdcEnrichedInstant] = {
//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forSpecificRecord(classOf[SdcEnrichedInstant]))
//				.withBucketCheckInterval(3L * 60L * 1000L)
////				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
//				.build()
//	}
	//	def buildSink(outputPath: String): StreamingFileSink[SdcRecord] = {
	//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(classOf[SdcRecord]))
	//				.withBucketAssigner(new DateTimeBucketAssigner[SdcRecord]("yyyy-MM-dd--HH-mm"))
	//				.build()
	//	}
//	def buildSink(outputPath: String): StreamingFileSink[String] = {
//		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(classOf[String]))
//				.withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd--HH-mm"))
//				.build()
//	}

	class SdcTimeBucketAssigner[T <: TemporalEvent](prefix: String, formatString: String) extends BucketAssigner[T, String]{
		@transient
		var dateFormatter = new SimpleDateFormat(formatString)

		override def getBucketId(in: T, context: BucketAssigner.Context): String = {
			if (dateFormatter == null) dateFormatter = new SimpleDateFormat(formatString)
			s"$prefix${dateFormatter.format(new java.util.Date(in.ts))}"
		}

		override def getSerializer = SimpleVersionedStringSerializer.INSTANCE
	}
}
