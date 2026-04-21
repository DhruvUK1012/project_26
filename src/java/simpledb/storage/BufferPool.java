package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.ArrayList;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;


    //
    private final Map<TransactionId, Set<TransactionId>> waitForGraph;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int numPages = DEFAULT_PAGES;

    private final ConcurrentHashMap<PageId, Page> pages;

    private static class Lock {
        private final Set<TransactionId> sharedHolders;
        private TransactionId exclusiveHolder;

        private Lock() {
            this.sharedHolders = new HashSet<>();
            this.exclusiveHolder = null;
        }
    }

    private final Map<PageId, Lock> pageLocks;
    private final Map<TransactionId, Set<PageId>> transactionLocks;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        BufferPool.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.pageLocks = new HashMap<>();
        this.transactionLocks = new HashMap<>();

        this.waitForGraph = new HashMap<>();
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        acquireLock(tid, pid, perm);

        Page page = pages.get(pid);
        if (page != null) {
            return page;
        }

        synchronized (this) {
            page = pages.get(pid);
            if (page != null) {
                return page;
            }

            if (pages.size() >= numPages) {
                evictPage();
            }

            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);
            pages.put(pid, page);
            return page;
        }
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
        // not necessary for lab1|lab2
        releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (this) {
            Lock lock = pageLocks.get(p);
            if (lock == null) {
                return false;
            }

            if (tid.equals(lock.exclusiveHolder)) {
                return true;
            }

            return lock.sharedHolders.contains(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        List <PageId> toProcess = new ArrayList<>();
        for(PageId pid : pages.keySet()){
                Page page = pages.get(pid);
                if (page.isDirty() != null && page.isDirty().equals(tid)){
                    toProcess.add(pid);
                }
            }
        
        for(PageId pid : toProcess){
            if (commit == true){
                try {
                    flushPage(pid);
                } catch(IOException e){
                    e.printStackTrace();
                }
            }else{
                discardPage(pid);
            }
        }
        List <PageId> toProcess = new ArrayList<>();
        for(PageId pid : pages.keySet()){
                Page page = pages.get(pid);
                if (page.isDirty() != null && page.isDirty().equals(tid)){
                    toProcess.add(pid);
                }
            }
        
        for(PageId pid : toProcess){
            if (commit == true){
                try {
                    flushPage(pid);
                } catch(IOException e){
                    e.printStackTrace();
                }
            }else{
                discardPage(pid);
            }
        }
        releaseAllLocks(tid);
        synchronized (this) {
            removeWaitEdges(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pages.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.deleteTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pages.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // not necessary for lab1
        for (PageId pid: pages.keySet()){
            Page page = pages.get(pid);
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // not necessary for lab1
        Page page = pages.get(pid);
        if (page == null) {
            return;
        }
        if (page.isDirty() == null){
            return;
        }
        if (page.isDirty() != null){
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
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
        // not necessary for lab1
        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);

            // In SimpleDB, prefer evicting a clean page
            if (page != null && page.isDirty() == null) {
                pages.remove(pid);
                return;
            }


        }

        throw new DbException("no clean page to evict");
    }

    private synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException {

        Lock lock = pageLocks.computeIfAbsent(pid, k -> new Lock());

        while (!canGrant(lock, tid, perm)) {
            Set<TransactionId> blockers = getBlockingTransactions(lock, tid, perm);

            addWaitEdges(tid, blockers);

            if (hasCycle(tid)) {
                removeOutgoingWaitEdges(tid);
                throw new TransactionAbortedException();
            }

            try {
                wait();
            } catch (InterruptedException e) {
                removeOutgoingOutgoingWaitEdges(tid);
                Thread.currentThread().interrupt();
                throw new TransactionAbortedException();
            }

            removeOutgoingOutgoingWaitEdges(tid);
        }

        removeOutgoingOutgoingWaitEdges(tid);

        if (perm == Permissions.READ_ONLY) {
            grantSharedLock(lock, tid, pid);
        } else {
            grantExclusiveLock(lock, tid, pid);
        }
    }

    private synchronized void releaseLock(TransactionId tid, PageId pid) {
        Lock lock = pageLocks.get(pid);
        if (lock == null) {
            return;
        }

        boolean changed = false;

        if (tid.equals(lock.exclusiveHolder)) {
            lock.exclusiveHolder = null;
            changed = true;
        }

        if (lock.sharedHolders.remove(tid)) {
            changed = true;
        }

        Set<PageId> heldPages = transactionLocks.get(tid);
        if (heldPages != null) {
            heldPages.remove(pid);
            if (heldPages.isEmpty()) {
                transactionLocks.remove(tid);
            }
        }

        if (lock.exclusiveHolder == null && lock.sharedHolders.isEmpty()) {
            pageLocks.remove(pid);
        }

        if (changed) {
            notifyAll();
        }
    }

    private synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> heldPages = transactionLocks.get(tid);
        if (heldPages == null || heldPages.isEmpty()) {
            return;
        }

        Set<PageId> pagesToRelease = new HashSet<>(heldPages);
        for (PageId pid : pagesToRelease) {
            releaseLock(tid, pid);
        }
    }

    private boolean canGrant(Lock lock, TransactionId tid, Permissions perm) {
        if (perm == Permissions.READ_ONLY) {
            return canGrantShared(lock, tid);
        } else {
            return canGrantExclusive(lock, tid);
        }
    }

    private boolean canGrantShared(Lock lock, TransactionId tid) {
        if (lock.exclusiveHolder == null) {
            return true;
        }
        return lock.exclusiveHolder.equals(tid);
    }

    private boolean canGrantExclusive(Lock lock, TransactionId tid) {
        if (tid.equals(lock.exclusiveHolder)) {
            return true;
        }

        if (lock.exclusiveHolder != null) {
            return false;
        }

        if (lock.sharedHolders.isEmpty()) {
            return true;
        }

        return lock.sharedHolders.size() == 1 && lock.sharedHolders.contains(tid);
    }

    private void grantSharedLock(Lock lock, TransactionId tid, PageId pid) {
        if (!tid.equals(lock.exclusiveHolder)) {
            lock.sharedHolders.add(tid);
        }
        recordTransactionLock(tid, pid);
    }

    private void grantExclusiveLock(Lock lock, TransactionId tid, PageId pid) {
        lock.sharedHolders.remove(tid);
        lock.exclusiveHolder = tid;
        recordTransactionLock(tid, pid);
    }

    private void recordTransactionLock(TransactionId tid, PageId pid) {
        transactionLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }


    //helper functions

    private Set<TransactionId> getBlockingTransactions(Lock lock, TransactionId tid, Permissions perm) {
        Set<TransactionId> blockers = new HashSet<>();

        if (perm == Permissions.READ_ONLY) {
            if (lock.exclusiveHolder != null && !lock.exclusiveHolder.equals(tid)) {
                blockers.add(lock.exclusiveHolder);
            }
        } else { // READ_WRITE
            if (lock.exclusiveHolder != null && !lock.exclusiveHolder.equals(tid)) {
                blockers.add(lock.exclusiveHolder);
            }

            for (TransactionId holder : lock.sharedHolders) {
                if (!holder.equals(tid)) {
                    blockers.add(holder);
                }
            }
        }

        return blockers;
    }

    private void addWaitEdges(TransactionId from, Set<TransactionId> toTransactions) {
        if (toTransactions.isEmpty()) {
            return;
        }
        waitForGraph.computeIfAbsent(from, k -> new HashSet<>()).addAll(toTransactions);
    }

    private void removeOutgoingWaitEdges(TransactionId tid) {
        waitForGraph.remove(tid);
    }

    private void removeOutgoingWaitEdges(TransactionId tid) {
        waitForGraph.remove(tid);
    }

    private void removeWaitEdges(TransactionId tid) {
        waitForGraph.remove(tid);
        for (Set<TransactionId> neighbors : waitForGraph.values()) {
            neighbors.remove(tid);
        }
    }

    private boolean hasCycle(TransactionId start) {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> stack = new HashSet<>();
        return dfsCycle(start, visited, stack);
    }

    private boolean dfsCycle(TransactionId current, Set<TransactionId> visited, Set<TransactionId> stack) {
        if (stack.contains(current)) {
            return true;
        }
        if (visited.contains(current)) {
            return false;
        }

        visited.add(current);
        stack.add(current);

        Set<TransactionId> neighbors = waitForGraph.get(current);
        if (neighbors != null) {
            for (TransactionId next : neighbors) {
                if (dfsCycle(next, visited, stack)) {
                    return true;
                }
            }
        }

        stack.remove(current);
        return false;
    }
}

