package com.starfox.analysis.copper.sdc.data

import java.sql.Timestamp
import java.time.Instant

import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter


/**
  * Created by Huyen on 5/9/18.
  */
class PojoAms(var object_name: String,
			  var customer_id: String,
			  var metrics_date: Timestamp) {
	def this () = this("", "", Timestamp.from(Instant.MIN))
}

object PojoAms {
	def unapply(arg: PojoAms): Option[(String, String)] = {
		arg.object_name.span(_ != ':') match {
			case (dslam, port) => Some ((dslam, port.drop(1)))
			case _ => None
		}
	}

	val SCHEMA = new Schema.Parser().parse("""{
		  "type" : "record",
		  "name" : "PojoAms",
		  "namespace" : "sess.cmd1Wrapper.Helper",
		  "fields" : [ {
		    "name" : "object_name",
		    "type" : "string"
		  }, {
		    "name" : "customer_id",
		    "type" : "string"
		  }, {
		    "name" : "metrics_date",
		    "type" : {
		      "type" : "long",
		      "logicalType" : "timestamp-millis"
		    }
		  } ]
		}""")
	val MESSAGE_TYPE = new AvroSchemaConverter().convert(SCHEMA)
}
