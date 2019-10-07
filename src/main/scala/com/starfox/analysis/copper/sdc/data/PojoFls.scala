package com.starfox.analysis.copper.sdc.data

import java.sql.Timestamp
import java.time.Instant

import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter

/**
  * Created by Huyen on 5/9/18.
  */
class PojoFls(var avc_id: String,
			  var ntd_id: String,
			  var uni_prid: String,
			  var access_service_tech_type: String,
			  var metrics_date: Timestamp,
			  var avc_bandwidth_profile: String) {

	def this() = this("", "", "", "", Timestamp.from(Instant.MIN), "")

	override def toString: String = s"FlsRaw($avc_id,$ntd_id,$uni_prid,$access_service_tech_type)"

//	val SCHEMA = new Schema.Parser().parse("""{
//		  "type" : "record",
//		  "name" : "PojoAms",
//		  "namespace" : "sess.cmd1Wrapper.Helper",
//		  "fields" : [ {
//		    "name" : "object_name",
//		    "type" : "string"
//		  }, {
//		    "name" : "customer_id",
//		    "type" : "string"
//		  }, {
//		    "name" : "metrics_date",
//		    "type" : {
//		      "type" : "long",
//		      "logicalType" : "timestamp-millis"
//		    }
//		  } ]
//		}""")
//	val MESSAGE_TYPE = new AvroSchemaConverter().convert(SCHEMA)
}