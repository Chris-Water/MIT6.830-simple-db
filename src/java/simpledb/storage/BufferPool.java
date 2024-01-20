package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.stream.Collectors;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {


    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final Integer numPages;
    private final Map<PageId, Page> buffer;
    /**
     * 该page对应的锁
     */
    private final Map<PageId, PageLock> locks;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        buffer = new LinkedHashMap<>(numPages);
        locks = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public static void main(String[] args) {

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        //为page初始化锁
        PageLock pageLock = locks.computeIfAbsent(pid, k -> new PageLock(pid));
        Page page;
        if (perm == Permissions.READ_ONLY) {
            //读请求
            try {
                while (!pageLock.trySharedLock(tid)) {
                    pageLock.await();
                }
                page = getPage(pid);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            //写请求
            //当持有读锁的事务进行写请求 ，若只有该事务持有读锁，则读锁升级为写锁
            try {
                while (!pageLock.tryExclusiveLock(tid)) {
                    pageLock.await();
                }
                page = getPage(pid);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return page;
    }

    private Page getPage(PageId pid) throws DbException {
        //用LRU算法作为淘汰策略
        Page page;
        if (!buffer.containsKey(pid)) {
            //缓存未命中
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);
            buffer.put(pid, page);
            if (buffer.size() > numPages) {
                evictPage();
            }
        } else {
            // 返回页面
            page = buffer.get(pid);
            buffer.remove(pid);
            buffer.put(pid, page);
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        PageLock pageLock = locks.get(pid);
        if (pageLock == null) {
            return;
        }
        pageLock.unlock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        //释放事务相关的所有锁
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here`
        return locks.containsKey(p) && locks.get(p).isHoldLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        pages.forEach(page -> {
            //标记脏页
            page.markDirty(true, tid);
            //将脏页添加到缓存
            buffer.put(page.getId(), page);
        });
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        pages.forEach(page -> {
            //标记脏页
            page.markDirty(true, tid);
            //将脏页添加到缓存
            buffer.put(page.getId(), page);
        });

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        //找出所有的脏页
        List<Page> dirtyPages = buffer.values().stream().filter(p -> p.isDirty() != null).collect(Collectors.toList());
        for (Page page : dirtyPages) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            dbFile.writePage(page);
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        if (buffer.containsKey(pid)) {
            Page page = buffer.get(pid);
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        //LRU移除pool头部的元素 如果是脏页需要刷盘
        PageId first = buffer.keySet().iterator().next();
        Page page = buffer.get(first);
        if (page.isDirty() != null) {
            try {
                flushPage(first);
            } catch (IOException e) {
                throw new DbException(e.getMessage());
            }
        }
        buffer.remove(first);
    }

    static class PageLock {
        final PageId pid;
        final Condition condition;
        /**
         * 0-无锁
         * 1-读锁
         * 2-写锁
         */
        volatile LockState state = LockState.NO_LOCK;
        volatile Set<TransactionId> holdLockTrans;

        public PageLock(PageId pid) {
            this.pid = pid;
            holdLockTrans = new HashSet<>();
            condition = new sync().condition;
        }

        public synchronized boolean trySharedLock(TransactionId tid) {
            if (state == LockState.NO_LOCK) {
                //无锁
                state = LockState.SHARED_LOCK;
                holdLockTrans.add(tid);
                return true;
            } else if (state == LockState.SHARED_LOCK) {
                //有读锁
                holdLockTrans.add(tid);
                return true;
            } else {
                //有写锁
                return holdLockTrans.contains(tid);
            }
        }

        public void await() throws InterruptedException {
            condition.await();
        }


        public synchronized boolean tryExclusiveLock(TransactionId tid) {
            if (state == LockState.NO_LOCK) {
                //无锁
                state = LockState.EXCLUSIVE_LOCK;
                holdLockTrans.add(tid);
                return true;
            } else if (state == LockState.SHARED_LOCK) {
                //当前为读锁
                // 持有者仅有当前tid时候upgrade为写锁
                if (holdLockTrans.size() == 1 && holdLockTrans.contains(tid)) {
                    state = LockState.EXCLUSIVE_LOCK;
                    return true;
                }
                return false;
            } else {
                //当前为写锁
                return holdLockTrans.contains(tid);
            }
        }

        public synchronized void unlock(TransactionId tid) {
            if (state == LockState.NO_LOCK) {
                return;
            }
            if (state == LockState.SHARED_LOCK) {
                holdLockTrans.remove(tid);
                if (holdLockTrans.isEmpty()) {
                    condition.signalAll();
                    state = LockState.NO_LOCK;
                }
                return;
            }
            if (state == LockState.EXCLUSIVE_LOCK) {
                holdLockTrans.remove(tid);
                condition.signalAll();
                state = LockState.NO_LOCK;

            }
        }

        public boolean isHoldLock(TransactionId tid) {
            return holdLockTrans.contains(tid);
        }

        enum LockState {
            NO_LOCK, SHARED_LOCK, EXCLUSIVE_LOCK
        }

        private class sync extends AbstractQueuedSynchronizer {

            public Condition condition = new ConditionObject();

            @Override
            protected boolean tryAcquire(int arg) {
                return true;
            }

            @Override
            protected boolean tryRelease(int arg) {
                return true;
            }

            @Override
            protected int tryAcquireShared(int arg) {
                return 0;
            }

            @Override
            protected boolean tryReleaseShared(int arg) {
                return true;
            }

            @Override
            protected boolean isHeldExclusively() {
                return true;
            }
        }
    }

}
