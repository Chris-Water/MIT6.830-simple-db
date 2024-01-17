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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc desc;
    private List<Tuple> toDelete;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        tid = t;
        this.child = child;
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
        toDelete = new ArrayList<>();
        while (child.hasNext()) {
            toDelete.add(child.next());
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (toDelete == null) {
            return null;
        }
        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();
        try {
            for (Tuple t : toDelete) {
                bufferPool.deleteTuple(tid, t);
                count++;
            }
            Tuple tuple = new Tuple(desc);
            tuple.setField(0, new IntField(count));
            toDelete = null;
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
