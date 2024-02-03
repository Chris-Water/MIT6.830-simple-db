package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and implements the Page interface
 * that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    private final Byte oldDataLock = (byte) 0;
    HeapPageId pid;
    byte[] oldData;
    private boolean isDirty;
    private TransactionId lastModifiedTid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk. The format of a HeapPage is a set
     * of header bytes indicating the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p> floor((BufferPool.getPageSize()*8) / (tuple
     * size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}. The number of 8-bit
     * header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Static method to generate a byte array corresponding to an empty HeapPage. Used to add new,
     * empty pages to the file. Passing the results of this method to the HeapPage constructor will
     * create a HeapPage with no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        return BufferPool.getPageSize() * 8 / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying
     * tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying
     * tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(numSlots / 8.0);

    }

    /**
     * Return a view of this page before it was modified -- used by recovery
     */
    @Override
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page. Used to serialize this page to
     * disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte array generated by
     * getPageData to the HeapPage constructor and have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {
            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }
            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length
                + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to
     * reflect that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        if (!pid.equals(t.getRecordId().getPageId())) {
            throw new DbException("tuple is not on this page");
        }
        //找到要删除的页的下标,将header对应位标记为未使用
        int idx = -1;
        for (int i = 0; i < tuples.length; i++) {
            Tuple tuple = tuples[i];
            if (tuple == null) {
                continue;
            }
            if (t.getRecordId().equals(tuple.getRecordId())) {
                idx = i;
                break;
            }
        }
        if (idx != -1) {
            if (!isSlotUsed(idx)) {
                throw new DbException("tuple is empty");
            }
            markSlotUsed(idx, false);
        } else {
            throw new DbException("tuple is not on this page");
        }
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect that it is now
     * stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        if (getNumEmptySlots() == 0) {
            throw new DbException("the page is full");
        }
        if (!td.equals(t.getTupleDesc())) {
            throw new DbException("tupleDesc is mismatch");
        }
        //找到一个未使用的槽
        OptionalInt emptySlot = IntStream.range(0, numSlots).filter(i -> !isSlotUsed(i)).findFirst();
        if (emptySlot.isPresent()) {
            tuples[emptySlot.getAsInt()] = t;
            markSlotUsed(emptySlot.getAsInt(), true);
            //更新插入元素的真实recordId
            t.setRecordId(new RecordId(pid, emptySlot.getAsInt()));
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        isDirty = dirty;
        lastModifiedTid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not
     * dirty
     */
    @Override
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return isDirty ? lastModifiedTid : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        return (int) IntStream.range(0, numSlots).filter(i -> !isSlotUsed(i)).count();
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        int idx = i / 8;
        int bit = i % 8;
        byte b = header[idx];
        //判断b的第bit位是否为1
        return (b & (1 << bit)) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // 把header中对应的bit 设置为 value
        int idx = i / 8;
        int bit = i % 8;
        byte b = header[idx];
        if (value) {
            //fill
            header[idx] = (byte) ((1 << bit) | b);
        } else {
            //clear
            header[idx] = (byte) (header[idx] & ~(1 << bit));
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an
     * UnsupportedOperationException) (note that this iterator shouldn't return tuples in empty
     * slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> tupleList = IntStream.range(0, numSlots).filter(this::isSlotUsed).mapToObj(idx -> tuples[idx]).collect(Collectors.toList());
        return tupleList.iterator();
    }

}

