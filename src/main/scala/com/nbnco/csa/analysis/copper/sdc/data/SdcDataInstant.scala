package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataInstant(ifAdminStatus: JBool,
                          ifOperStatus: JBool,
                          actualDs: Integer,
                          actualUs: Integer,
                          attndrDs: Integer,
                          attndrUs: Integer,
                          attenuationDs: Option[Short],
                          userMacAddress: String
                         ) {
    def toMap: Map[String, Any] = {
        Map (
            "ifAdminStatus" -> ifAdminStatus,
            "ifOperStatus" -> ifOperStatus,
            "xdslFarEndChannelActualNetDataRateDownstream" -> actualDs,
            "xdslChannelActualNetDataRateUpstream" -> actualUs,
            "xdslFarEndChannelAttainableNetDataRateDownstream" -> attndrDs,
            "xdslChannelAttainableNetDataRateUpstream" -> attndrUs,
            "xdslFarEndLineLoopAttenuationDownstream" -> attenuationDs.map(_/10.0f: JFloat).orNull,
            "macaddress" -> userMacAddress
        )
    }
}

object SdcDataInstant {
    val EMPTY = SdcDataInstant(false, false, -1, -1, -1, -1, None, null)
    def apply(): SdcDataInstant = EMPTY
}
