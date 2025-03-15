package org.foxdb.file;

public class BlockID {
    private String fileName;
    private int blockNumber;

    public BlockID(String fileName, int blockNumber) {
        this.fileName = fileName;
        this.blockNumber = blockNumber;
    }

    public String getFileName() {
        return fileName;
    }

    public int getBlockNumber() {
        return blockNumber;
    }
}
