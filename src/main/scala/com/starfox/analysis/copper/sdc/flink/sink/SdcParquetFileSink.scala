package com.starfox.analysis.copper.sdc.flink.sink

import java.text.SimpleDateFormat

import com.starfox.analysis.copper.sdc.data._
import org.apache.avro.Schema
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

import scala.reflect.ClassTag

/**
  * Created by Huyen on 4/10/18.
  */
object SdcParquetFileSink {
	def buildSinkReflect[T <: CopperLine](outputPath: String, prefix: String, suffixFormat: String)(implicit ct: ClassTag[T]): StreamingFileSink[T] = {
		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(ct.runtimeClass)).asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
//				.withBucketCheckInterval(3L * 60L * 1000L)
				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
				.build()
	}

	def buildSinkGeneric[T <: TemporalEvent](schema: Schema, outputPath: String, prefix: String, suffixFormat: String): StreamingFileSink[T] = {
		StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forGenericRecord(schema))
				.asInstanceOf[StreamingFileSink.BulkFormatBuilder[T, String]]
				//				.withBucketCheckInterval(3L * 60L * 1000L)
				.withBucketAssigner(new SdcTimeBucketAssigner[T](prefix, suffixFormat))
				.build()
	}

	class SdcTimeBucketAssigner[T <: TemporalEvent](prefix: String, formatString: String) extends BucketAssigner[T, String]{
		@transient
		lazy val dateFormatter = new SimpleDateFormat(formatString)

		override def getBucketId(in: T, context: BucketAssigner.Context): String = {
//			if (dateFormatter == null) dateFormatter = new SimpleDateFormat(formatString)
			s"$prefix${dateFormatter.format(new java.util.Date(in.ts))}"
		}

		override def getSerializer = SimpleVersionedStringSerializer.INSTANCE
	}
}
