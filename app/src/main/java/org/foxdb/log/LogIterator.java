package org.foxdb.log;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.file.SlottedPage;

import java.io.IOException;
import java.util.Iterator;

public class LogIterator{

    private FileManager fm;
    private BlockID blk;
    private String logFile;
    private int currentPos = 0;
    private SlottedPage p;
    public LogIterator(FileManager fm, String logFile){
        this.fm = fm;
        this.logFile = logFile;
        this.p = new SlottedPage(fm.getBlockSize());
        try {
            this.blk = new BlockID(this.logFile, fm.fileLength(this.logFile) - 1);
            fm.read(this.blk, p.getPage());
            currentPos = p.length()-1;
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }
    private void moveToBlock(BlockID blk){
        try {
            fm.read(blk, p.getPage());
            currentPos = p.length()-1;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public boolean hasNext() {
        return this.blk.getBlockNumber() > 0 || currentPos >=0;
    }

    public byte[] next() {
        currentPos = Math.min(currentPos, this.p.length()-1);
        if(currentPos < 0){
            this.blk = new BlockID(logFile, this.blk.getBlockNumber()-1);
            moveToBlock(this.blk);
        }

        byte[] rec = p.get(currentPos);
        currentPos--;
        return rec;
    }

    public boolean hasPrevious(){

        try {
            int length = fm.fileLength(this.logFile);
            int currentBlockNumber = this.blk.getBlockNumber();
            int pageLength = this.p.length();
            return currentBlockNumber < length - 1 || currentPos < pageLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] previous(){
        currentPos = Math.max(currentPos,0);
        if(currentPos >= this.p.length()){
            this.blk = new BlockID(logFile, this.blk.getBlockNumber()+1);
            moveToBlock(this.blk);
            currentPos = 0;
        }

        byte[] rec = p.get(currentPos);
        currentPos++;
        return rec;
    }
}
