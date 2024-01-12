package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    DbFileIterator dbFileIterator;
    TupleDesc tupleDesc;
    private int tableId;
    private String tableAlias;
    private TransactionId transactionId;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        tableId = tableid;
        transactionId = tid;
        this.tableAlias = tableAlias;
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableId).iterator(transactionId);
        tupleDesc = getTupleDesc(tableId, this.tableAlias);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * 根据表的别名生成元组描述
     *
     * @param tableId
     * @param tableAlias
     * @return
     */
    private TupleDesc getTupleDesc(int tableId, String tableAlias) {
        TupleDesc desc = Database.getCatalog().getTupleDesc(tableId);
        int count = desc.numFields();
        Iterator<TupleDesc.TDItem> it = desc.iterator();
        Type[] types = new Type[count];
        String[] fieldName = new String[count];
        for (int i = 0; i < count; i++) {
            TupleDesc.TDItem item = it.next();
            types[i] = item.fieldType;
            fieldName[i] = tableAlias + "." + item.fieldName;
        }
        return new TupleDesc(types, fieldName);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        tableId = tableid;
        this.tableAlias = tableAlias;
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(transactionId);
        tupleDesc = getTupleDesc(tableId, this.tableAlias);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return dbFileIterator.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        return dbFileIterator.next();
    }

    @Override
    public void close() {
        // some code goes here
        dbFileIterator.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        dbFileIterator.rewind();
    }
}
