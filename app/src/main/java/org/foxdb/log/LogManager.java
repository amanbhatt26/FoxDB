package org.foxdb.log;

import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class LogManager {
    private FileManager fm;
    private Page logPage;
    private BlockID currentBlock;
    private String logFile;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fm, String logFile){
        this.fm = fm;
        this.logFile = logFile;
        this.logPage = new Page(fm.getBlockSize());
        try{
            int fileLength = fm.fileLength(logFile);
            if(fileLength == 0){
                this.currentBlock = appendNewBlock();
            }else{
                this.currentBlock = new BlockID(logFile, fileLength -1);
                fm.read(this.currentBlock, this.logPage);
            }
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    private BlockID appendNewBlock() {
        try{
            BlockID blk = fm.appendBlock(this.logFile);
            logPage.setInt(0, fm.getBlockSize());
            fm.write(blk, logPage);
            return blk;

        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    public synchronized int append(byte[] logRec){
        int boundary = logPage.getInt(0);
        int recSize = logRec.length + Integer.BYTES;

        if(boundary - recSize < Integer.BYTES){
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }

        int logLocation = boundary - recSize;
        logPage.setBytes(logLocation, logRec);
        logPage.setInt(0, logLocation);
        latestLSN += 1;
        return latestLSN;

    }

    public void flush(int lsn){
        if(lsn >= lastSavedLSN) flush();
    }
    private void flush(){
        try{
            fm.write(this.currentBlock, this.logPage);
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public Iterator<byte[]> iterator(){
        flush();
        return new LogIterator(this.fm, this.logFile);
    }

}
