package com.nbnco.csa.analysis.copper.sdc.flink.operator;

import com.nbnco.csa.analysis.copper.sdc.data.DslamCompact;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.*;

public class AggregateRawDslamJavaTableAPI extends AggregateFunction<DslamCompact, DslamCompact> {

    @Override
    public DslamCompact createAccumulator() {
        return new DslamCompact();
    }

    @Override
    public DslamCompact getValue(DslamCompact acc) {
        return acc;
    }


    public void merge(DslamCompact acc, Iterable<DslamCompact> it) {
        Iterator<DslamCompact> iter = it.iterator();
        while (iter.hasNext()) {
            DslamCompact a = iter.next();
            acc.columns += "," + a.columns;
            acc.records = mergeCsv(acc.columns, acc.records, a.columns, a.records);
        }
    }

    public void accumulate(DslamCompact acc, boolean isInstant, String columns, Map<String, String> records) throws Exception {
        byte type = (byte) (isInstant ? 1 : 2);
        if (acc.type == 3 || acc.type == type) {
            throw new Exception("ABC");
        } else {
            acc.type += type;
            if (acc.type < 3) {
                acc.columns = columns;
                acc.records = records;
            } else {
                acc.columns += "," + columns;
                acc.records = mergeCsv(acc.columns, acc.records, columns, records);
            }
        }
    }

    private Map<String, String> mergeCsv(String c1, Map<String, String> r1, String c2, Map<String, String> r2) {
        String d1blank = c1.chars().filter(r -> r == ',').toString();
        String d2blank = c2.chars().filter(r -> r == ',').toString();
        Set<String> i = new HashSet<>(r1.keySet());
        i.addAll(r2.keySet());

        Map<String, String> ret = new HashMap<>();
        i.forEach(k -> ret.put(k, r1.getOrDefault(k, d1blank) + "," + r1.getOrDefault(k, d2blank)));
        return ret;
    }
}
//
//
//class AggregateRawDslamScala extends AggregateFunction[DslamCompactWithType, DslamCompactWithType] {
//
//        override def createAccumulator(): DslamCompactWithType = {
//        new DslamCompactWithType(0: Byte, 0L, "", "", Map.empty)
//        }
//
//        override def getValue(acc: DslamCompactWithType): DslamCompactWithType = acc
//
//        // isInstant, ts, name, columns, data
//        def accumulate(acc: DslamCompactWithType, in: DslamCompactWithType) = {
//        if (acc.f0 == 3 || acc.f0 == in.f0) {
//        //Raise exception here
//        throw InvalidDataException(s"Duplicated data in $DslamMetadata, ${in.f0} at ${in.f1}")
//        } else {
//        val (c, r) = MergeCsv(acc.f3, acc.f4, in.f3, in.f4)
//        acc.f0 += in.f0
//        acc.f1 = in.f1
//        acc.f2 = in.f2
//        acc.f3 = c
//        acc.f4 = r
//        }
//        }
//        }

