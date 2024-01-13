package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /**
     * Map< gField,aggregationTuple >
     */
    private Map<Field, Tuple> map = new HashMap<>();
    private Map<Field, Integer> countAggregation = new HashMap<>();
    private Map<Field, Integer> sumAggregation = new HashMap<>();
    private Type gbType;
    private int agfield;
    private int gbfield;
    private Op op;
    private TupleDesc desc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.agfield = afield;
        gbType = gbfieldtype;
        op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
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
                Type[] types = {aggregation.getType()};
                String[] name = {String.format("%s(%s)", op, tup.getTupleDesc().getFieldName(agfield))};
                desc = new TupleDesc(types, name);
                agTuple = new Tuple(desc);
            } else {
                Type[] types = {gbType, aggregation.getType()};
                String[] name = {tup.getTupleDesc().getFieldName(gbfield), String.format("%s(%s)", op, tup.getTupleDesc().getFieldName(agfield))};
                desc = new TupleDesc(types, name);
                agTuple = new Tuple(desc);
                agTuple.setField(0, gb);
            }
            switch (op) {
                case MIN:
                case MAX:
                case AVG:
                case SUM:
                    agTuple.setField(idx, aggregation);
                    break;
                case COUNT:
                    agTuple.setField(idx, new IntField(1));
                    break;
                default:

            }
            map.put(gb, agTuple);
            countAggregation.put(gb, 1);
            sumAggregation.put(gb, aggregation.hashCode());
        } else {
            Tuple oldTuple = map.get(gb);
            int count = countAggregation.get(gb) + 1;
            int sum = sumAggregation.get(gb) + aggregation.hashCode();
            switch (op) {
                case MIN:
                    oldTuple.setField(idx, aggregation.compare(Predicate.Op.LESS_THAN, oldTuple.getField(1)) ? aggregation : oldTuple.getField(1));
                    break;
                case MAX:
                    oldTuple.setField(idx, aggregation.compare(Predicate.Op.GREATER_THAN, oldTuple.getField(1)) ? aggregation : oldTuple.getField(1));
                    break;
                case AVG:
                    oldTuple.setField(idx, new IntField(sum / count));
                    break;
                case SUM:
                    oldTuple.setField(idx, new IntField(sum));
                    break;
                case COUNT:
                    oldTuple.setField(idx, new IntField(count));
                    break;
                default:
            }
            map.put(gb, oldTuple);
            countAggregation.put(gb, count);
            sumAggregation.put(gb, sum);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    @Override
    public OpIterator iterator() {
        // some code goes here
        return new TupleIterator(desc, map.values());
    }

}
