package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc aggregateTupleDesc;
    private OpIterator child;
    private int aggregateField;
    private int groupingField;
    private Aggregator.Op op;
    private Aggregator aggregator;
    private OpIterator it = null;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        aggregateField = afield;
        groupingField = gfield;
        op = aop;
        Type afieldType = child.getTupleDesc().getFieldType(aggregateField);
        Type gfieldType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        aggregator = afieldType == Type.INT_TYPE ? new IntegerAggregator(gfield, gfieldType, afield, op) : new StringAggregator(gfield, gfieldType, afield, op);
        //构造tupleDesc
        if (gfield == Aggregator.NO_GROUPING) {
            Type[] types = {Type.INT_TYPE};
            String[] name = {String.format("%s(%s)", op, aggregateFieldName())};
            aggregateTupleDesc = new TupleDesc(types, name);
        } else {
            Type[] types = {gfieldType, Type.INT_TYPE};
            String[] name = {groupFieldName(), String.format("%s(%s)", op, aggregateFieldName())};
            aggregateTupleDesc = new TupleDesc(types, name);
        }

    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupingField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return groupingField == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldName(groupingField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    @Override
    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        //合并到聚合
        while (child.hasNext()) {
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        it = aggregator.iterator();
        it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = aggregator.iterator();
        it.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return aggregateTupleDesc;
    }

    @Override
    public void close() {
        // some code goes here
        super.close();
        it = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
