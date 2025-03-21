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

    public boolean equals(Object obj){
        BlockID other = (BlockID) obj;
        return this.fileName.equals(other.getFileName()) && this.blockNumber == other.getBlockNumber();
    }

    public int hashCode(){
        return this.fileName.hashCode()*31 + this.blockNumber;
    }
}
