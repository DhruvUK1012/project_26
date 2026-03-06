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

public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Map<Field, Integer> countMap = new HashMap<>();

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) throw new IllegalArgumentException("StringAggregator only supports COUNT");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        countMap.put(key, countMap.getOrDefault(key, 0) + 1);
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
            Tuple t = new Tuple(td);
            int c = countMap.get(key);

            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(c));
            } else {
                t.setField(0, key);
                t.setField(1, new IntField(c));
            }
            tuples.add(t);
        }

        return new TupleIterator(td, tuples);
    }
}