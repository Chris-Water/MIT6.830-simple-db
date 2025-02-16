package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

  private int tableId;
  private int pageNumber;

  /**
   * Constructor. Create a page id structure for a specific page of a specific table.
   *
   * @param tableId The table that is being referenced
   * @param pgNo    The page number in that table.
   */
  public HeapPageId(int tableId, int pgNo) {
    // some code goes here
    this.tableId = tableId;
    pageNumber = pgNo;
  }

  /**
   * @return the table associated with this PageId
   */
  @Override
  public int getTableId() {
    // some code goes here
    return tableId;
  }

  /**
   * @return the page number in the table getTableId() associated with this PageId
   */
  @Override
  public int getPageNumber() {
    // some code goes here
    return pageNumber;
  }

  /**
   * @return a hash code for this page, represented by a combination of the table number and the
   * page number (needed if a PageId is used as a key in a hash table in the BufferPool, for
   * example.)
   * @see BufferPool
   */
  @Override
  public int hashCode() {
    // some code goes here
    return Objects.hash(tableId, pageNumber);
  }

  /**
   * Compares one PageId to another.
   *
   * @param o The object to compare against (must be a PageId)
   * @return true if the objects are equal (e.g., page numbers and table ids are the same)
   */
  @Override
  public boolean equals(Object o) {
    // some code goes here
    if (!(o instanceof PageId)) {
      return false;
    }
    PageId pageId = (PageId) o;
    return pageId.getTableId() == tableId && pageId.getPageNumber() == pageNumber;
  }

  /**
   * Return a representation of this object as an array of integers, for writing to disk.  Size of
   * returned array must contain number of integers that corresponds to number of args to one of the
   * constructors.
   */
  @Override
  public int[] serialize() {
    int[] data = new int[2];

    data[0] = getTableId();
    data[1] = getPageNumber();

    return data;
  }

  @Override
  public String toString() {
    return "pid=" + "(" + tableId + "," + pageNumber + ")";
  }
}
