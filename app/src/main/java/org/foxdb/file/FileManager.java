package org.foxdb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private int blockSize;
    private File dbDirectory;
    Map<String, RandomAccessFile> openFiles;
    public FileManager(File dbDirectory , int blockSize){
        this.dbDirectory = dbDirectory;
        if(!this.dbDirectory.exists()){
            this.dbDirectory.mkdirs();
        }
        openFiles = new HashMap<>();
        this.blockSize = blockSize;
    }

    private RandomAccessFile getFile(String filename) throws IOException{
        RandomAccessFile f = null;
        if(openFiles.containsKey(filename)){
            f = openFiles.get(filename);
        }else{
            f = new RandomAccessFile(filename, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }

    public synchronized void read(BlockID bid, Page p) throws IOException{

        RandomAccessFile f= getFile(bid.getFileName());

        int blockNumber = bid.getBlockNumber();
        String fileName = bid.getFileName();
        if(blockNumber >= fileLength(fileName) || blockNumber < 0){
            throw new IOException("Cannot read non existing block: " + blockNumber);
        }
        f.seek((long) bid.getBlockNumber() *blockSize);

        ByteBuffer bb = p.getByteBuffer();
        f.read(bb.array());
    }

    public synchronized  void write(BlockID bid, Page p) throws IOException{
        RandomAccessFile f= getFile(bid.getFileName());

        int blockNumber = bid.getBlockNumber();
        String fileName = bid.getFileName();
        if(blockNumber >= fileLength(fileName) || blockNumber < 0){
            throw new IOException("Cannot write to non existing block: " + blockNumber);
        }
        f.seek(bid.getBlockNumber()*blockSize);
        ByteBuffer bb = p.getByteBuffer();
        f.write(bb.array());
    }

    public int getBlockSize(){
        return blockSize;
    }

    public int fileLength(String fileName) throws IOException{
        RandomAccessFile file = getFile(fileName);
        return (int) file.length() / blockSize;
    }
    public BlockID appendBlock(String fileName) throws IOException{
        RandomAccessFile file = getFile(fileName);
        int length = fileLength(fileName);
        file.setLength((long)(length+1)*blockSize);
        BlockID block = new BlockID(fileName, fileLength(fileName) - 1);
        return block;
    }

}
