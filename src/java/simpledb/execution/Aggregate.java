package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator it;
    private TupleDesc outTd;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        TupleDesc childTd = child.getTupleDesc();

        Type gbType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);
        Type aType = childTd.getFieldType(afield);

        if (aType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gbType, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gbType, afield, aop);
        }

        String aggName = nameOfAggregatorOp(aop) + "(" + childTd.getFieldName(afield) + ")";

        if (gfield == Aggregator.NO_GROUPING) {
            outTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggName});
        } else {
            String gbName = childTd.getFieldName(gfield);
            outTd = new TupleDesc(new Type[]{gbType, Type.INT_TYPE}, new String[]{gbName, aggName});
        }
    }

    public int groupField() {
        return gfield;
    }

    public String groupFieldName() {
        if (gfield == Aggregator.NO_GROUPING) return null;
        return outTd.getFieldName(0);
    }

    public int aggregateField() {
        return afield;
    }

    public String aggregateFieldName() {
        if (gfield == Aggregator.NO_GROUPING) return outTd.getFieldName(0);
        return outTd.getFieldName(1);
    }

    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        super.open();

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }

        it = aggregator.iterator();
        it.open();
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (it == null) return null;
        if (it.hasNext()) return it.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (it != null) it.rewind();
    }

    public TupleDesc getTupleDesc() {
        return outTd;
    }

    public void close() {
        if (it != null) it.close();
        child.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}