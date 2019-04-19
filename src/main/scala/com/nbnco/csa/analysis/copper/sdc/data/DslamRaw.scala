package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 1/10/18.
  */
case class DslamRaw[DataType](metadata: DslamMetadata, data: Map[String, DataType])