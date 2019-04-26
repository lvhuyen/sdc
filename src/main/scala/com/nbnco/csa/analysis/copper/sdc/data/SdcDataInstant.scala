package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataInstant(var ifAdminStatus: JBool,
                          var ifOperStatus: JBool,
                          var actualDs: Integer,
                          var actualUs: Integer,
                          var attndrDs: Integer,
                          var attndrUs: Integer,
                          var attenuationDs: JShort,
                          var userMacAddress: String
                         ) extends SdcDataBase {
    def toMap: Map[String, Any] = {
        Map (
            "ifAdminStatus" -> ifAdminStatus,
            "ifOperStatus" -> ifOperStatus,
            "xdslFarEndChannelActualNetDataRateDownstream" -> actualDs,
            "xdslChannelActualNetDataRateUpstream" -> actualUs,
            "xdslFarEndChannelAttainableNetDataRateDownstream" -> attndrDs,
            "xdslChannelAttainableNetDataRateUpstream" -> attndrUs,
            "xdslFarEndLineLoopAttenuationDownstream" -> (if (attenuationDs == null) null else attenuationDs / 10.0f),
            "macaddress" -> userMacAddress
        )
    }
}

object SdcDataInstant {
    val EMPTY = SdcDataInstant(false, false, null, null, null, null, null, null)
    def apply(): SdcDataInstant = EMPTY
}
