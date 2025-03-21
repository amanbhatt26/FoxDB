package org.foxdb.transaction;

import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.concurrency.ConcurrencyManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.SlottedPage;
import org.foxdb.recovery.RecoveryManager;

import java.util.*;

public class Transaction {

    public enum Isolation{
        READ_UNCOMMITTED,
        READ_COMMITTED,
        READ_REPEATABLE,
        SERIALIZABLE
    }
    private ConcurrencyManager cm;
    private RecoveryManager rm;
    private Isolation isolation;
    private BufferManager bm;
    private int txId;
    private Set<Buffer> pinnedBuffers;
    private Set<BlockID> lockedBlocks;
    public Transaction(Isolation isolation, ConcurrencyManager cm, RecoveryManager rm, BufferManager bm, int txId){
        /* tell recovery manager to log my starting */
        this.isolation = isolation;
        this.cm = cm;
        this.rm = rm;
        this.txId = txId;
        this.bm = bm;
        this.pinnedBuffers = new HashSet();
        this.lockedBlocks = new HashSet();
        rm.logStart(this.txId);
    }

    public Transaction(ConcurrencyManager cm, RecoveryManager rm, BufferManager bm, int txId){
        this(Isolation.READ_COMMITTED, cm,rm, bm, txId);
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
    }


}
