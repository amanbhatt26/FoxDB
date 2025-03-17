package org.foxdb.buffer;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.log.LogManager;

// Responsible for reading Blocks from disk and caching them transparently
// Anyone who want to read some disk block , come to buffer manager
public class BufferManager {

    private FileManager fm;
    private LogManager lm;

    private Buffer[] buffers;

    private final int MAX_WAIT_TIME = 10000;

    public BufferManager(FileManager fm, LogManager lm, int bufferPoolSize){
        buffers = new Buffer[bufferPoolSize];
        for(int i=0;i<bufferPoolSize ;i++){
            buffers[i] = new Buffer(fm, lm);
        }
        this.fm = fm;
        this.lm = lm;
    }
    public void flushAll(int txnum){
        for(var buff:buffers){
            if(buff.lastModifiedTxn() == txnum ) buff.flush();
        }
    }


    public synchronized Buffer pin(BlockID blkid){
        try {
            Buffer buff = tryToPin(blkid);
            long startTime = System.currentTimeMillis();
            while(buff==null && !waitingTooLong(startTime)){
                wait(MAX_WAIT_TIME);
                buff = tryToPin(blkid);
            }
            if(buff==null){
                throw new RuntimeException();
            }

            return buff;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void unpin(Buffer buff){
        buff.unpin();
        if(!buff.isPinned()) notifyAll();
    }
    private Buffer tryToPin(BlockID blkid){
        // search if this blockID already exists in the buffer pool

        for(var buff:buffers){
            BlockID bid = buff.block();
            if(bid!=null && bid.equals(blkid)){
                buff.pin();
                return buff;
            }
        }

        return chooseFromUnpinned(blkid);
    }

    private Buffer chooseFromUnpinned(BlockID blkid){

        for(var buff:buffers){
            if(!buff.isPinned()){
                buff.assignToBlock(blkid);
                buff.pin();
                return buff;
            }
        }
        return null;
    }


    private boolean waitingTooLong(long startTime){
        return System.currentTimeMillis() - startTime > (long) MAX_WAIT_TIME;
    }


    public int numUnpinnedBuffers(){

        int count = 0;
        for(var buff:buffers){
            count += buff.isPinned() ?0:1;
        }
        return count;
    }


}
