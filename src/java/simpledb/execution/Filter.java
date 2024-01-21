package simpledb.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;
  private final Predicate predicate;
  private final TupleDesc tupleDesc;
  private OpIterator child;
  private List<Tuple> tuples = new ArrayList<>();
  private Iterator<Tuple> it = null;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   *
   * @param p     The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate p, OpIterator child) {
    // some code goes here
    predicate = p;
    tupleDesc = child.getTupleDesc();
    this.child = child;
  }

  public Predicate getPredicate() {
    // some code goes here
    return predicate;
  }

  @Override
  public TupleDesc getTupleDesc() {
    // some code goes here
    return tupleDesc;
  }

  @Override
  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    // some code goes here
    child.open();
    while (child.hasNext()) {
      Tuple next = child.next();
      if (predicate.filter(next)) {
        tuples.add(next);
      }
    }
    it = tuples.iterator();
    super.open();
  }

  @Override
  public void close() {
    // some code goes here
    super.close();
    it = null;
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    // some code goes here
    it = tuples.iterator();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator,
   * applying the predicate to them and returning those that pass the predicate (i.e. for which the
   * Predicate.filter() returns true.)
   *
   * @return The next tuple that passes the filter, or null if there are no more tuples
   * @see Predicate#filter
   */
  @Override
  protected Tuple fetchNext()
      throws NoSuchElementException, TransactionAbortedException, DbException {
    // some code goes here
    if (it != null && it.hasNext()) {
      return it.next();
    }
    return null;
  }

  @Override
  public OpIterator[] getChildren() {
    // some code goes here
    return new OpIterator[]{this.child};
  }

  @Override
  public void setChildren(OpIterator[] children) {
    // some code goes here
    this.child = children[0];
  }

}
