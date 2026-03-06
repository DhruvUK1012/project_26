package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Map<Field, Integer> countMap = new HashMap<>();
    private final Map<Field, Integer> sumMap = new HashMap<>();
    private final Map<Field, Integer> minMap = new HashMap<>();
    private final Map<Field, Integer> maxMap = new HashMap<>();

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int v = ((IntField) tup.getField(afield)).getValue();

        int c = countMap.getOrDefault(key, 0) + 1;
        countMap.put(key, c);

        sumMap.put(key, sumMap.getOrDefault(key, 0) + v);

        if (!minMap.containsKey(key) || v < minMap.get(key)) {
            minMap.put(key, v);
        }
        if (!maxMap.containsKey(key) || v > maxMap.get(key)) {
            maxMap.put(key, v);
        }
    }

    public OpIterator iterator() {
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        List<Tuple> tuples = new ArrayList<>();

        for (Field key : countMap.keySet()) {
            int aggVal;
            switch (what) {
                case COUNT:
                    aggVal = countMap.get(key);
                    break;
                case SUM:
                    aggVal = sumMap.get(key);
                    break;
                case AVG:
                    aggVal = sumMap.get(key) / countMap.get(key);
                    break;
                case MIN:
                    aggVal = minMap.get(key);
                    break;
                case MAX:
                    aggVal = maxMap.get(key);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported aggregation op: " + what);
            }

            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(aggVal));
            } else {
                t.setField(0, key);
                t.setField(1, new IntField(aggVal));
            }
            tuples.add(t);
        }

        return new TupleIterator(td, tuples);
    }
}