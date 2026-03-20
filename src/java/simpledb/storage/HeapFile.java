package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();
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
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];

            RandomAccessFile raf = new RandomAccessFile(file, "r");
            int offset = ((HeapPageId) pid).getPageNumber() * pageSize;

            raf.seek(offset);
            raf.readFully(data);
            raf.close();

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        int pageNo = page.getId().getPageNumber();
        int offset = pageNo * pageSize;

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();

        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                modifiedPages.add(page);
                return modifiedPages;
            }
        }

        HeapPageId newPid = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);

        HeapPage insertedPage = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        modifiedPages.add(insertedPage);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();

        RecordId rid = t.getRecordId();
        if (rid == null) {
            throw new DbException("tuple has no record id");
        }

        HeapPageId pid = (HeapPageId) rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        modifiedPages.add(page);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private int pageNo;
        private Iterator<Tuple> tupleIterator;
        private boolean open;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            open = true;
            pageNo = 0;
            loadPage();
        }

        private void loadPage() throws DbException, TransactionAbortedException {
            if (pageNo >= numPages()) {
                tupleIterator = null;
                return;
            }
            HeapPageId pid = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open || tupleIterator == null) return false;

            if (tupleIterator.hasNext()) return true;

            while (++pageNo < numPages()) {
                loadPage();
                if (tupleIterator != null && tupleIterator.hasNext()) return true;
            }
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();
            return tupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            tupleIterator = null;
            open = false;
        }
    }
}