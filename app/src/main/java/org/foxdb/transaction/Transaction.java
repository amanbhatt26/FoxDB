package org.foxdb.transaction;

import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.concurrency.ConcurrencyManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.SlottedPage;
import org.foxdb.recovery.RecoveryManager;

import java.io.IOException;
import java.util.*;

public class Transaction {

    public enum Isolation{
        READ_UNCOMMITTED,
        READ_COMMITTED,
        READ_REPEATABLE,
        SERIALIZABLE
    }
    private final ConcurrencyManager cm;
    private final RecoveryManager rm;
    private final Isolation isolation;
    private final BufferManager bm;
    private final FileManager fm;
    private final int txId;
    private final Set<Buffer> pinnedBuffers;
    private final Set<BlockID> lockedBlocks;
    private final Set<String> lockedEOFs;
    public Transaction(Isolation isolation, ConcurrencyManager cm, RecoveryManager rm, BufferManager bm, FileManager fm, int txId){
        /* tell recovery manager to log my starting */
        this.isolation = isolation;
        this.cm = cm;
        this.rm = rm;
        this.txId = txId;
        this.bm = bm;
        this.fm = fm;
        this.pinnedBuffers = new HashSet();
        this.lockedBlocks = new HashSet();
        this.lockedEOFs = new HashSet();
        rm.logStart(this.txId);
    }

    public Transaction(ConcurrencyManager cm, RecoveryManager rm, BufferManager bm, FileManager fm, int txId){
        this(Isolation.READ_COMMITTED, cm,rm, bm, fm, txId);
    }

    public boolean fitCheck(BlockID blk, byte[] b){
        Buffer buff = bm.pin(blk);
        pinnedBuffers.add(buff);
        if(isolation !=Isolation.READ_UNCOMMITTED) {
            cm.sLock(blk, txId);
            this.lockedBlocks.add(blk);
        }
        SlottedPage sp = new SlottedPage(buff.contents());

        if(isolation  == Isolation.READ_COMMITTED){
            cm.unlock(blk, txId);
            this.lockedBlocks.remove(blk);
        }

        return sp.canInsert(b);
    }
    public byte[] read(BlockID blk,int slotIndex){
        Buffer buff = bm.pin(blk);
        pinnedBuffers.add(buff);
        if(isolation !=Isolation.READ_UNCOMMITTED) {
            cm.sLock(blk, txId);
            this.lockedBlocks.add(blk);
        }
        SlottedPage sp = new SlottedPage(buff.contents());

        if(isolation  == Isolation.READ_COMMITTED){
            cm.unlock(blk, txId);
            this.lockedBlocks.remove(blk);
        }
        return sp.get(slotIndex);
    }

    public void insert(BlockID blk,int slotIndex, byte[] b){
        Buffer buff = bm.pin(blk);
        pinnedBuffers.add(buff);
        cm.xLock(blk, txId);
        this.lockedBlocks.add(blk);
        SlottedPage sp = new SlottedPage(buff.contents());
        sp.defragment();
        sp.put(slotIndex, b);
        int lsn = rm.logInsert(txId, blk, slotIndex, b);

        buff.setModified(txId, lsn);
    }

    public void remove(BlockID blk, int slotIndex){

        Buffer buff = bm.pin(blk);
        pinnedBuffers.add(buff);
        cm.xLock(blk, txId);
        this.lockedBlocks.add(blk);
        SlottedPage sp = new SlottedPage(buff.contents());
        byte[] b = sp.get(slotIndex);
        sp.remove(slotIndex);

        int lsn = rm.logRemove(txId, blk, slotIndex, b);
        buff.setModified(txId, lsn);
    }

    public void update(BlockID blk, int slotIndex, byte[] b){
        Buffer buff = bm.pin(blk);
        pinnedBuffers.add(buff);
        cm.xLock(blk, txId);
        this.lockedBlocks.add(blk);
        SlottedPage sp = new SlottedPage(buff.contents());
        byte[] oldB = sp.get(slotIndex);
        sp.update(slotIndex, b);

        int lsn = rm.logUpdate(txId, blk, slotIndex, oldB, b);
        buff.setModified(txId, lsn);
    }

    public void commit(){
        rm.commit(txId);
        cleanUp();
    }

    public void rollback(){
        rm.rollback(txId);
        cleanUp();
    }

    public BlockID appendNewBlock(String fileName){
        // Exclusive lock on EOF marker
        cm.xLock(fileName, txId);
        this.lockedEOFs.add(fileName);
        try {
            return fm.appendBlock(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int fileLength(String fileName){
        // shared lock on EOF marker
        if(isolation == Isolation.SERIALIZABLE) {
            cm.sLock(fileName, txId);
            this.lockedEOFs.add(fileName);
        }
        try {
            int fLength = fm.fileLength(fileName);
            return fm.fileLength(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public int slotArrayLength(BlockID blk){
        try {
            Buffer buff = bm.pin(blk);
            pinnedBuffers.add(buff);
            // shared lock on EOF marker
            if(isolation == Isolation.SERIALIZABLE) {
                cm.sLock(blk, txId);
                this.lockedBlocks.add(blk);
            }
            SlottedPage sp = new SlottedPage(buff.contents());
            return sp.length();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanUp(){
        /* let go of all the held locks */
        Iterator<BlockID> blockIter = this.lockedBlocks.iterator();
        while(blockIter.hasNext()){
            cm.unlock(blockIter.next(), txId);
        }

        /* let go of all the pinned buffers */
        Iterator<Buffer> buffIter = this.pinnedBuffers.iterator();
        while(buffIter.hasNext()){
            bm.unpin(buffIter.next());
        }

        Iterator<String> eofIter = this.lockedEOFs.iterator();
        while(eofIter.hasNext()){
            cm.unlock(eofIter.next(), txId);
        }
    }


}
