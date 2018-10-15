package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 1/10/18.
  */
case class DslamRaw[DataType](ts: Long, dslam: String, filename: String, header: String, data: Seq[(String, DataType)]) {

}
