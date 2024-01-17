package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;
    //private final Map<Integer, Page> pages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        file = f;
        //pages = new HashMap<>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        int startPosition = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = HeapPage.createEmptyPageData();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r")) {
            //查询偏移量
            randomAccessFile.seek(startPosition);
            randomAccessFile.read(data, 0, data.length);
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            //pages.putIfAbsent(pid.hashCode(), page);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs

    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        //根据页号计算偏移量
        int startPosition = page.getId().getPageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw")) {
            //找到写入位置
            randomAccessFile.seek(startPosition);
            randomAccessFile.write(data, 0, data.length);
        }
        //pages.putIfAbsent(page.getId().hashCode(), page);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        HeapPage page;
        int pgNo = -1;
        // 找有空槽的Page插入该tuple
        do {
            pgNo++;
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
            //pages.putIfAbsent(page.getId().hashCode(), page);
        } while (page.getNumEmptySlots() == 0);
        HeapPageId newPageId = new HeapPageId(recordId.getPageId().getTableId(), pgNo);
        //t.setRecordId(new RecordId(newPageId, recordId.getTupleNumber()));
        //pages.putIfAbsent(newPageId.hashCode(), page);
        page.insertTuple(t);
        if (pgNo >= numPages()) {
            writePage(page);
        }
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> it = null;
        HeapPage curp = null;
        TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            //在bufferPool 获取page 跳过空页
            int pgNo = -1;
            do {
                pgNo++;
                curp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_ONLY);
            } while (curp.numSlots == curp.getNumEmptySlots());
            //curp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), 0), Permissions.READ_ONLY);
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
                if (pNo >= numPages()) {
                    curp = null;
                } else {
                    curp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pNo), Permissions.READ_ONLY);
                    //跳过空页面
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


}

