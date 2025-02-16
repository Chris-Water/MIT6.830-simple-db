package simpledb.transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

  static final AtomicLong counter = new AtomicLong(0);
  private static final long serialVersionUID = 1L;
  final long myid;

  public TransactionId() {
    myid = counter.getAndIncrement();
  }

  public long getId() {
    return myid;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TransactionId other = (TransactionId) obj;
    return myid == other.myid;
  }

  @Override
  public String toString() {
    return "tid=" + myid;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (myid ^ (myid >>> 32));
    return result;
  }
}
