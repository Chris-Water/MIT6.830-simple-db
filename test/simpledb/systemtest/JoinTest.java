package simpledb.systemtest;

import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinTest extends SimpleDbTestBase {
    private static final int COLUMNS = 2;

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(JoinTest.class);
    }

    public static void main(String[] args) {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void validateJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue, int table2Rows) throws IOException, DbException, TransactionAbortedException {
        // Create the two tables
        Map<Integer, Integer> columnSpecification = new HashMap<>();
        columnSpecification.put(0, table1ColumnValue);
        List<List<Integer>> t1Tuples = new ArrayList<>();
        HeapFile table1 = SystemTestUtil.createRandomHeapFile(COLUMNS, table1Rows, columnSpecification, t1Tuples);
        assert t1Tuples.size() == table1Rows;

        columnSpecification.put(0, table2ColumnValue);
        List<List<Integer>> t2Tuples = new ArrayList<>();
        HeapFile table2 = SystemTestUtil.createRandomHeapFile(COLUMNS, table2Rows, columnSpecification, t2Tuples);
        assert t2Tuples.size() == table2Rows;

        // Generate the expected results
        List<List<Integer>> expectedResults = new ArrayList<>();
        for (List<Integer> t1 : t1Tuples) {
            for (List<Integer> t2 : t2Tuples) {
                // If the columns match, join the tuples
                if (t1.get(0).equals(t2.get(0))) {
                    List<Integer> out = new ArrayList<>(t1);
                    out.addAll(t2);
                    expectedResults.add(out);
                }
            }
        }

        // Begin the join
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "");
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinOp = new Join(p, ss1, ss2);

        // test the join results
        SystemTestUtil.matchTuples(joinOp, expectedResults);

        joinOp.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    @Test
    public void testSingleMatch() throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 1, 1, 1);
    }

    @Test
    public void testNoMatch() throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 2, 2, 10);
    }

    @Test
    public void testMultipleMatch() throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 3, 1, 3);
    }

}
