package com.nbnco.csa.analysis.copper.sdc.data

import com.nbnco.csa.analysis.copper.sdc.utils.InvalidDataException

/**
  * Created by Huyen on 17/8/18.
  */

/**
  */
case class SdcCombined(ts: Long, dslam: String, port: String,
                       enrich: SdcDataEnrichment,
                       dataI: SdcDataInstant,
                       dataH: SdcDataHistorical
                ) extends SdcEnrichedBase {

    def this() = this (0L, "", "",
        SdcDataEnrichment(),
        SdcDataInstant(),
        SdcDataHistorical())
}

object SdcCombined {
    def apply(i: SdcEnrichedInstant, h: SdcEnrichedHistorical): SdcCombined = {
        if (i.dslam != h.dslam || i.port != h.port || i.ts != h.ts)
            throw new InvalidDataException("Creating combined SDC record with non-matching members")
        if (i.enrich.tsEnrich > h.enrich.tsEnrich)
            new SdcCombined(i.ts, i.dslam, i.port, i.enrich, i.data, h.data)
        else
            new SdcCombined(h.ts, h.dslam, h.port, h.enrich, i.data, h.data)
    }
    def apply(input: SdcEnrichedBase): SdcCombined = {
        input match {
            case i: SdcEnrichedInstant => SdcCombined(i, SdcEnrichedHistorical())
            case h: SdcEnrichedHistorical =>  SdcCombined(SdcEnrichedInstant(), h)
        }
    }
}


