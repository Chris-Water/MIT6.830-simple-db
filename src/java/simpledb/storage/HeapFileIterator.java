package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final HeapFile f;
    Iterator<Tuple> it = null;
    HeapPage curp = null;

    /**
     * Constructor for this iterator
     *
     * @param f   - the heapFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        //在bufferPool 获取page
        curp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), 0), Permissions.READ_ONLY);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext()) {
            it = null;
        }
        while (it == null && curp != null) {
            //找到下一页的PageNo
            int pNo = curp.getId().getPageNumber() + 1;
            if (pNo >= f.numPages()) {
                curp = null;
            } else {
                curp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pNo), Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext()) {
                    it = null;
                }
            }
        }
        return it == null ? null : it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    @Override
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}

