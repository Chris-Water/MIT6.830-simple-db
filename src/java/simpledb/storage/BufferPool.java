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
import java.util.stream.IntStream;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call
 * into it to retrieve pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches a page, BufferPool
 * checks that the transaction has the appropriate locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {


    /**
     * Default number of pages passed to the constructor. This is used by other classes. BufferPool
     * should use the numPages argument to the constructor instead.
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

    private final Map<TransactionId, Set<PageId>> transRelevantPages;

    private DeadLockDetectGraph deadLockDetectGraph;

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
        transRelevantPages = new ConcurrentHashMap<>();
        deadLockDetectGraph = new DeadLockDetectGraph();
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
        LinkedList<Integer> list = new LinkedList<Integer>();
        IntStream.range(0, 5).forEach(list::add);
        ListIterator<Integer> it = list.listIterator();
        while (it.hasNext()) {
            Integer next = it.next();
            System.out.println(next);
            it.add(1);
        }
        System.out.println(list);
    }

    public void printDeadLockDetectionGraph() {
        System.out.println("==================================DeadLock Detection Graph==================================");
        deadLockDetectGraph.printGraph();
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a lock and may block
     * if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it is present, it should be
     * returned.  If it is not present, it should be added to the buffer pool and returned.  If there
     * is insufficient space in the buffer pool, a page should be evicted and the new page should be
     * added in its place.
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

                while (!pageLock.trySharedLock(tid, deadLockDetectGraph)) {
                    //获取锁失败则加入等待队列
                    pageLock.wait();
                }

                page = getPage(pid, tid);
                //事务获取page成功 加入 page->trans
                deadLockDetectGraph.acquirePage(tid, pid);
                return page;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            //写请求
            //当持有读锁的事务进行写请求 ，若只有该事务持有读锁，则读锁升级为写锁
            try {

                while (!pageLock.tryExclusiveLock(tid, deadLockDetectGraph)) {
                    //获取锁失败则加入等待队列
                    pageLock.wait();
                }

                page = getPage(pid, tid);
                //事务获取page成功 加入 page->trans
                deadLockDetectGraph.acquirePage(tid, pid);
                return page;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * 打印各个page的锁的状态
     */
    public void printLockState() {
        System.out.println("==================================Lock State==================================");
        locks.values().forEach(System.out::println);
    }

    private Page getPage(PageId pid, TransactionId tid) throws DbException {
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
            // 从缓存获取页面 执行LRU
            page = buffer.get(pid);
            buffer.remove(pid);
            buffer.put(pid, page);
        }
        //缓存事务相关page
        transRelevantPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
        return page;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior.
     * Think hard about who needs to call this and why, and why they can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
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
        transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        System.out.println(tid + " is " + (commit ? "commit" : "abort"));
        Set<PageId> relevantPages = transRelevantPages.get(tid);
        if (commit) {
            //commit 将事务相关的page刷新到磁盘
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (relevantPages != null) {
                //abort 恢复事务相关的dirty page到未修改状态
                relevantPages.stream().filter(buffer::containsKey).map(buffer::get).filter(page -> page.isDirty() != null).forEach(page -> buffer.put(page.getId(), page.getBeforeImage()));
            }
        }
        //释放事务相关的所有page的锁
        if (relevantPages != null) {
            relevantPages.forEach(pid -> unsafeReleasePage(tid, pid));
        }
        //从死锁检测图中移除事务
        deadLockDetectGraph.transFinish(tid);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here`
        return locks.containsKey(p) && locks.get(p).isHoldLock(tid);
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will acquire a write lock on
     * the page the tuple is added to and any other pages that are updated (Lock acquisition is not
     * needed for lab2). May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and
     * adds versions of any pages that have been dirtied to the cache (replacing any existing versions
     * of those pages) so that future requests see up-to-date pages.
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
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the
     * tuple is removed from and any other pages that are updated. May block if the lock(s) cannot be
     * acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and
     * adds versions of any pages that have been dirtied to the cache (replacing any existing versions
     * of those pages) so that future requests see up-to-date pages.
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
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to
     * disk so will break simpledb if running in NO STEAL mode.
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
     * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that
     * the buffer pool doesn't keep a rolled back page in its cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages are removed from the cache so they can
     * be reused safely
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
        Set<PageId> pageIds = transRelevantPages.get(tid);
        if (pageIds == null) {
            return;
        }
        for (PageId pid : pageIds) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are
     * updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        //LRU移除pool中一个最久未使用的非脏页
        //事务的机制 要求不能evict脏页
        Iterator<PageId> it = buffer.keySet().iterator();
        Page page = null;
        PageId pid = null;
        while (it.hasNext()) {
            pid = it.next();
            page = buffer.get(pid);
            if (page.isDirty() == null) {
                //如果找到干净页
                PageLock pageLock = locks.get(pid);
                for (TransactionId tid : pageLock.getHoldLockTrans()) {
                    unsafeReleasePage(tid, pid);
                }
                buffer.remove(pid);
                locks.remove(pid);
                return;
            }
        }
        if (page != null && page.isDirty() != null) {
            //如果全是脏页则抛出异常
            throw new DbException("all pages are dirty");
        }
    }

    private static class PageLock {

        final PageId pid;
        final Condition condition;
        private volatile Set<TransactionId> holdLockTrans;
        private volatile LockState state = LockState.NO_LOCK;

        public PageLock(PageId pid) {
            this.pid = pid;
            holdLockTrans = new HashSet<>();
            condition = new sync().condition;
        }

        /**
         * 锁状态
         *
         * @return 返回锁状态
         */
        public LockState lockState() {
            return state;
        }

        public Set<TransactionId> getHoldLockTrans() {
            return Collections.unmodifiableSet(holdLockTrans);
        }

        public synchronized boolean trySharedLock(TransactionId tid, DeadLockDetectGraph deadLockDetectGraph) throws TransactionAbortedException {
            //往死锁检测图 事务请求page 加入边 trans->page
            deadLockDetectGraph.requestPage(tid, pid);
            //检测死锁
            DeadLockDetectGraph.Node deadLockNode = deadLockDetectGraph.detectDeadLock();
            if (deadLockNode != null) {
                //存在死锁
                // abort当前事务
                // Database.getBufferPool().transactionComplete(tid, false);
                //return null;
                throw new TransactionAbortedException();
            }
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

        public synchronized void await() throws InterruptedException {
            condition.await();
        }

        public synchronized boolean tryExclusiveLock(TransactionId tid, DeadLockDetectGraph deadLockDetectGraph) throws TransactionAbortedException {
            //往死锁检测图 事务请求page 加入边 trans->page
            deadLockDetectGraph.requestPage(tid, pid);
            //检测死锁
            DeadLockDetectGraph.Node deadLockNode = deadLockDetectGraph.detectDeadLock();
            if (deadLockNode != null) {
                //存在死锁
                // abort当前事务
                // Database.getBufferPool().transactionComplete(tid, false);
                //return null;
                Database.getBufferPool().printLockState();
                Database.getBufferPool().printDeadLockDetectionGraph();
                throw new TransactionAbortedException();
            }
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
            if (state == LockState.NO_LOCK || !holdLockTrans.contains(tid)) {
                //如果无锁 / 事务不持有这个锁
                return;
            }
            if (state == LockState.SHARED_LOCK) {
                holdLockTrans.remove(tid);
                this.notify();
                if (holdLockTrans.isEmpty()) {
                    state = LockState.NO_LOCK;
                }
                return;
            }
            if (state == LockState.EXCLUSIVE_LOCK) {
                holdLockTrans.remove(tid);
                this.notify();
                state = LockState.NO_LOCK;
            }
        }

        public synchronized boolean isHoldLock(TransactionId tid) {
            return state != LockState.NO_LOCK && holdLockTrans.contains(tid);
        }

        @Override
        public String toString() {
            return "PageLock{" + pid + ", holdLockTrans=" + holdLockTrans + ", state=" + state + '}';
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

    static class DeadLockDetectGraph {

        private final Map<Node, Set<Node>> graph;

        public DeadLockDetectGraph() {
            graph = new ConcurrentHashMap<>();
        }


        /**
         * 事务请求Page资源
         *
         * @param tid
         * @param pid
         */
        public synchronized void requestPage(TransactionId tid, PageId pid) {
            Node<TransactionId> trans = new Node<>(tid, Type.TRANSACTION);
            Node<PageId> page = new Node<>(pid, Type.PAGE);
            //判断pid是否已经被Trans持有
            if (graph.get(page) != null && graph.get(page).contains(trans)) {
                return;
            }
            Set<Node> transEdge = graph.computeIfAbsent(trans, k -> new HashSet<>());
            Set<Node> pageEdge = graph.computeIfAbsent(page, k -> new HashSet<>());
            //trans->page
            transEdge.add(page);
        }

        public synchronized void transFinish(TransactionId tid) {
            Node<TransactionId> trans = new Node<>(tid, Type.TRANSACTION);
            //从顶点和边中删除 graph
            graph.remove(trans);
            for (Set<Node> edges : graph.values()) {
                edges.remove(trans);
            }
        }

        /**
         * 事务获得Page资源
         *
         * @param tid
         * @param pid
         */
        public synchronized void acquirePage(TransactionId tid, PageId pid) {
            Node<TransactionId> trans = new Node<>(tid, Type.TRANSACTION);
            Node<PageId> page = new Node<>(pid, Type.PAGE);
            //判断pid是否已经被Trans持有
            Set<Node> pageEdge = graph.get(page);
            if (pageEdge != null && pageEdge.contains(trans)) {
                return;
            }
            //移除 trans->page的边
            if (graph.containsKey(trans)) {
                Set<Node> transEdge = graph.get(trans);
                transEdge.remove(page);
            }
            pageEdge = graph.computeIfAbsent(page, k -> new HashSet<>());
            //page->trans
            pageEdge.add(trans);
        }

        /**
         * 检测图中是否有环
         *
         * @return 如果无环返回null 有环返回环中的某个节点
         */
        public synchronized Node detectDeadLock() {
            //利用拓扑排序 每轮优先选择入度为0的节点加入有序列表
            List<Node> topSort = new ArrayList<>();
            Map<Node, Set<Node>> clone = new ConcurrentHashMap<>(graph);
            while (!clone.isEmpty()) {
                Set<Node> zeroEntry = new HashSet<>(clone.keySet());
                //找到入度为0的顶点
                for (Set<Node> edge : clone.values()) {
                    zeroEntry.removeIf(edge::contains);
                }
                if (zeroEntry.isEmpty()) {
                    //没有找到入度为0的点 存在环
                    return clone.keySet().iterator().next();
                }
                //加入排序结果 从图中移除
                topSort.addAll(zeroEntry);
                //移除顶点
                zeroEntry.forEach(clone::remove);
                //移除边
                for (Set<Node> edge : clone.values()) {
                    edge.removeAll(zeroEntry);
                }
            }
            //System.out.println("TopSort:" + topSort);
            return null;
        }


        public void printGraph() {
            graph.forEach((k, v) -> {
                System.out.println(k + " " + v);
            });
        }

        enum Type {
            PAGE, TRANSACTION
        }

        static class Node<E> {

            final Type type;
            final E id;

            Node(E id, Type type) {
                this.id = id;
                this.type = type;
            }


            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Node<?> node = (Node<?>) o;
                return type == node.type && Objects.equals(id, node.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(type, id);
            }

            @Override
            public String toString() {
                return type + "(" + id + ")";
            }
        }
    }

}
