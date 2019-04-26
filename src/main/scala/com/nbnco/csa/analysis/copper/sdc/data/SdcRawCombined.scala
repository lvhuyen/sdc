//package com.nbnco.csa.analysis.copper.sdc.data
//
///**
//  * Created by Huyen on 11/7/18.
//  */
//
///**
//  * @param dslam
//  * @param ts               Timestamp: The time when data is measured. To be used as event_time
//  * @param port
//  */
//case class SdcRawCombined(ts: Long, dslam: String, port: String,
//                         data: SdcDataCombined
//                   ) extends SdcRawBase  {
//
//    def this () = this (0L, "", "", SdcDataInstant())
//
//    def enrich(enrich: EnrichmentData): SdcCombined = {
//        SdcCombined(this, enrich)
//    }
//}
