package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private Map<Field, Tuple> map = new HashMap<>();
    private Map<Field, Integer> countAggregation = new HashMap<>();
    private Type gbType;
    private int agfield;
    private int gbfield;
    private Op op;
    private TupleDesc desc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringField only supports COUNT");
        }
        this.gbfield = gbfield;
        this.agfield = afield;
        gbType = gbfieldtype;
        op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggregation = tup.getField(agfield);
        Field gb = gbfield == Aggregator.NO_GROUPING ? null : tup.getField(gbfield);
        int idx = gbfield == Aggregator.NO_GROUPING ? 0 : 1;
        if (!map.containsKey(gb)) {
            Tuple agTuple;
            if (gbfield == Aggregator.NO_GROUPING) {
                //未group by
                Type[] types = {Type.INT_TYPE};
                String[] name = {String.format("%s(%s)", op, tup.getTupleDesc().getFieldName(agfield))};
                desc = new TupleDesc(types, name);
                agTuple = new Tuple(desc);
            } else {
                Type[] types = {gbType, Type.INT_TYPE};
                String[] name = {tup.getTupleDesc().getFieldName(gbfield), String.format("%s(%s)", op, tup.getTupleDesc().getFieldName(agfield))};
                desc = new TupleDesc(types, name);
                agTuple = new Tuple(desc);
                agTuple.setField(0, gb);
            }
            agTuple.setField(idx, new IntField(1));
            map.put(gb, agTuple);
            countAggregation.put(gb, 1);
        } else {
            Tuple oldTuple = map.get(gb);
            int count = countAggregation.get(gb) + 1;
            oldTuple.setField(idx, new IntField(count));
            map.put(gb, oldTuple);
            countAggregation.put(gb, count);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        // some code goes here
        return new TupleIterator(desc, map.values());
    }

}
