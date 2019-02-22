package com.nbnco.csa.analysis.copper.sdc.data

/**
  * Created by Huyen on 30/9/18.
  */
case class SdcDataInstant(var if_admin_status: String,
                          var if_oper_status: String,
                          var actual_ds: Int,
                          var actual_us: Int,
                          var attndr_ds: Int,
                          var attndr_us: Int,
                          var attenuation_ds: Integer,
                          var user_mac_address: String
                         ) extends SdcDataBase {
    def toMap: Map[String, Any] = {
        Map (
            "ifAdminStatus" -> if_admin_status,
            "ifOperStatus" -> if_oper_status,
            "xdslFarEndChannelActualNetDataRateDownstream" -> actual_ds,
            "xdslChannelActualNetDataRateUpstream" -> actual_us,
            "xdslFarEndChannelAttainableNetDataRateDownstream" -> attndr_ds,
            "xdslChannelAttainableNetDataRateUpstream" -> attndr_us,
            "xdslFarEndLineLoopAttenuationDownstream" -> attenuation_ds,
            "macaddress" -> user_mac_address
        )
    }
}

object SdcDataInstant {
    def apply(): SdcDataInstant = SdcDataInstant("down", "down", -1, -1, -1, -1, -1, null)
}
