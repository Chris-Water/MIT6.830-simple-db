package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final int tableId;
    private final TupleDesc desc;
    private OpIterator child;
    private List<Tuple> toInsert = null;


    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // some code goes here
        tid = t;
        this.child = child;
        this.tableId = tableId;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("TupleDesc of tuple differs from table");
        }
        desc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        toInsert = new ArrayList<>();
        while (child.hasNext()) {
            toInsert.add(child.next());
        }
    }

    @Override
    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (toInsert == null) {
            return null;
        }
        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();
        try {
            for (Tuple t : toInsert) {
                bufferPool.insertTuple(tid, tableId, t);
                count++;
            }
            Tuple tuple = new Tuple(desc);
            tuple.setField(0, new IntField(count));
            toInsert = null;
            return tuple;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
