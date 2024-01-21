package simpledb.optimizer;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

  static final int IO_COST_PER_PAGE = 1000;
  /**
   * Number of bins for the histogram. Feel free to increase this value over 100, though our tests
   * assume that you have at least 100 bins in your histograms.
   */
  static final int NUM_HIST_BINS = 100;
  /**
   * Map<tableName, TableStats>
   */
  private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();
  private final int IoCostPerPage;
  private final int tableId;
  private final TupleDesc tupleDesc;
  private Object[] grams;
  private int[][] minAndMax;
  private DbFile dbFile;
  private int tupleNum;

  /**
   * Create a new TableStats object, that keeps track of statistics on each column of a table
   *
   * @param tableid       The table over which to compute statistics
   * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
   *                      sequential-scan IO and disk seeks.
   */
  public TableStats(int tableid, int ioCostPerPage) {
    // For this function, you'll have to get the
    // DbFile for the table in question,
    // then scan through its tuples and calculate
    // the values that you need.
    // You should try to do this reasonably efficiently, but you don't
    // necessarily have to (for example) do everything
    // in a single scan of the table.
    // some code goes here
    tableId = tableid;
    IoCostPerPage = ioCostPerPage;
    dbFile = Database.getCatalog().getDatabaseFile(tableid);
    tupleDesc = dbFile.getTupleDesc();
    DbFileIterator it = dbFile.iterator(new TransactionId());

    int numFields = tupleDesc.numFields();
    minAndMax = new int[numFields][2];
    try {
      it.open();
      //扫描一遍记录每个field的最小和最大值
      while (it.hasNext()) {
        Tuple next = it.next();
        //记录表内的tuple行数
        tupleNum++;
        //记录每个field的最大值和最小值
        for (int i = 0; i < tupleDesc.numFields(); i++) {
          Field field = next.getField(i);
          if (field.getType() == Type.INT_TYPE) {
            IntField f = (IntField) field;
            int value = f.getValue();
            minAndMax[i][0] = Math.min(value, minAndMax[i][0]);
            minAndMax[i][1] = Math.max(value, minAndMax[i][1]);
          }
        }
      }
      // 创建直方图
      grams = new Object[numFields];
      for (int i = 0; i < numFields; i++) {
        Type type = tupleDesc.getFieldType(i);
        grams[i] = type == Type.INT_TYPE ? new IntHistogram(NUM_HIST_BINS, minAndMax[i][0],
            minAndMax[i][1]) : new StringHistogram(NUM_HIST_BINS);
      }
      //将记录加入直方图
      it.rewind();
      while (it.hasNext()) {
        Tuple next = it.next();
        //将tuple的每个field加入对应的直方图
        for (int i = 0; i < numFields; i++) {
          Field field = next.getField(i);
          if (field.getType() == Type.INT_TYPE) {
            IntHistogram gram = (IntHistogram) grams[i];
            IntField f = (IntField) field;
            gram.addValue(f.getValue());
          } else {
            StringHistogram gram = (StringHistogram) grams[i];
            StringField f = (StringField) field;
            gram.addValue(f.getValue());
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      it.close();
    }

  }

  public static TableStats getTableStats(String tablename) {
    return statsMap.get(tablename);
  }

  public static void setTableStats(String tablename, TableStats stats) {
    statsMap.put(tablename, stats);
  }

  public static Map<String, TableStats> getStatsMap() {
    return statsMap;
  }

  public static void setStatsMap(Map<String, TableStats> s) {
    try {
      java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
      statsMapF.setAccessible(true);
      statsMapF.set(null, s);
    } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException |
             SecurityException e) {
      e.printStackTrace();
    }

  }

  public static void computeStatistics() {
    Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

    System.out.println("Computing table stats.");
    while (tableIt.hasNext()) {
      int tableid = tableIt.next();
      TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
      setTableStats(Database.getCatalog().getTableName(tableid), s);
    }
    System.out.println("Done.");
  }

  /**
   * Estimates the cost of sequentially scanning the file, given that the cost to read a page is
   * costPerPageIO. You can assume that there are no seeks and that no pages are in the buffer
   * pool.
   * <p>
   * Also, assume that your hard drive can only read entire pages at once, so if the last page of
   * the table only has one tuple on it, it's just as expensive to read as a full page. (Most real
   * hard drives can't efficiently address regions smaller than a page at a time.)
   *
   * @return The estimated cost of scanning the table.
   */
  public double estimateScanCost() {
    // some code goes here
    //扫描的pageNum * IoCost
    return dbFile.numPages() * IoCostPerPage;
  }

  /**
   * This method returns the number of tuples in the relation, given that a predicate with
   * selectivity selectivityFactor is applied.
   *
   * @param selectivityFactor The selectivity of any predicates over the table
   * @return The estimated cardinality of the scan with the specified selectivityFactor
   */
  public int estimateTableCardinality(double selectivityFactor) {
    // some code goes here
    return (int) (totalTuples() * selectivityFactor);
  }

  /**
   * The average selectivity of the field under op.
   *
   * @param field the index of the field
   * @param op    the operator in the predicate The semantic of the method is that, given the table,
   *              and then given a tuple, of which we do not know the value of the field, return the
   *              expected selectivity. You may estimate this value from the histograms.
   */
  public double avgSelectivity(int field, Predicate.Op op) {
    // some code goes here
    return 1.0;
  }

  /**
   * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
   *
   * @param field    The field over which the predicate ranges
   * @param op       The logical operation in the predicate
   * @param constant The value against which the field is compared
   * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
   */
  public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    // some code goes here
    Type type = tupleDesc.getFieldType(field);
    double result = 0;
    if (type == Type.INT_TYPE) {
      IntHistogram gram = (IntHistogram) grams[field];
      result = gram.estimateSelectivity(op, ((IntField) constant).getValue());
    } else {
      StringHistogram gram = (StringHistogram) grams[field];
      result = gram.estimateSelectivity(op, ((StringField) constant).getValue());
    }
    return result;
  }

  /**
   * return the total number of tuples in this table
   */
  public int totalTuples() {
    // some code goes here
    return tupleNum;
  }

}
