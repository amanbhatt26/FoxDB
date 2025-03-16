package org.foxdb.log;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;

import java.io.IOException;
import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {

    private FileManager fm;
    private BlockID blk;
    private String logFile;
    private int currentPos = 0;
    private Page p;
    public LogIterator(FileManager fm, String logFile){
        this.fm = fm;
        this.logFile = logFile;
        this.p = new Page(fm.getBlockSize());
        try {
            this.blk = new BlockID(this.logFile, fm.fileLength(this.logFile) - 1);
            fm.read(this.blk, p);
            currentPos = p.getInt(0);
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }
    private void moveToBlock(BlockID blk){
        try {
            fm.read(blk, p);
            currentPos = p.getInt(0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return this.blk.getBlockNumber() > 0 || currentPos < fm.getBlockSize();
    }

    @Override
    public byte[] next() {
        if(currentPos >= fm.getBlockSize()){
            this.blk = new BlockID(logFile, this.blk.getBlockNumber()-1);
            moveToBlock(this.blk);
        }

        byte[] rec = p.getBytes(currentPos);
        currentPos += rec.length + Integer.BYTES;
        return rec;
    }
}
