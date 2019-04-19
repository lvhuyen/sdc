package com.nbnco.csa.analysis.copper.sdc.data;

import java.util.Map;

public class DslamCompact {
    public byte type = 0;
//    public long ts = 0;
//    public String name = null;
    public String columns = null;
    public Map<String, String> records = null;

    @Override
    public String toString() {
        return "(Dslam: " + type + "," + records.size() + ")";
    }
}
