package org.foxdb.log;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.file.SlottedPage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class LogManager {
    private FileManager fm;
    private SlottedPage logPage;
    private BlockID currentBlock;
    private String logFile;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fm, String logFile){
        this.fm = fm;
        this.logFile = logFile;
        this.logPage = new SlottedPage(fm.getBlockSize());
        try{
            int fileLength = fm.fileLength(logFile);
            if(fileLength == 0){
                this.currentBlock = appendNewBlock();
            }else{
                this.currentBlock = new BlockID(logFile, fileLength -1);
                fm.read(this.currentBlock, this.logPage.getPage());
            }
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    private BlockID appendNewBlock() {
        try{
            BlockID blk = fm.appendBlock(this.logFile);
            this.logPage = new SlottedPage(fm.getBlockSize());
            fm.write(blk, this.logPage.getPage());
            return blk;

        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    public synchronized int append(byte[] logRec){
        int length = logPage.length();

        if(!logPage.canInsert(logRec)){
            this.currentBlock = appendNewBlock();
        }
        this.logPage.put(length, logRec);
        latestLSN += 1;
        return latestLSN;

    }

    public void flush(int lsn){
        if(lsn >= lastSavedLSN) flush();
    }
    private void flush(){
        try{
            fm.write(this.currentBlock, this.logPage.getPage());
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public LogIterator iterator(){
        flush();
        return new LogIterator(this.fm, this.logFile);
    }

}
