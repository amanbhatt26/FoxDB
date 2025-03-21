package org.foxdb.buffer;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.log.LogManager;

import java.io.IOException;

public class Buffer {
    private FileManager fm;
    private LogManager lm;
    private int lsn = -1;
    private int txn = -1;
    private Page p;
    int numPins = 0;
    BlockID blkid;

    public Buffer(FileManager fm, LogManager lm){
        this.fm = fm;
        this.lm = lm;
        this.p = new Page(fm.getBlockSize());
    }

    public void assignToBlock(BlockID blkid){
        flush();
        this.blkid = blkid;
        try {
            fm.read(this.blkid, this.p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        numPins = 0;

    }

    void flush(){
        if(txn >=0){
            lm.flush(lsn);
            try {
                fm.write(this.blkid, this.p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            txn = -1;
        }
    }

    public boolean isPinned(){
        return numPins > 0;
    }
    void pin(){
        numPins++;
    }

    void unpin(){
        if(numPins > 0) numPins--;
    }

    public void setModified(int txn, int lsn){
        this.txn = txn;
        this.lsn = lsn;
    }
    public BlockID block(){
        return blkid;
    }
    public Page contents(){
        return p;
    }

    public int lastModifiedTxn(){
        return txn;
    }


}
