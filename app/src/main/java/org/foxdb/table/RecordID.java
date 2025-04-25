package org.foxdb.table;

import org.foxdb.file.BlockID;
import org.foxdb.file.Page;

public class RecordID {
    public static final int SIZE=20;
    private BlockID blkid;
    private int slotNumber;

    public RecordID(BlockID blkid, int slotNumber) {
        this.blkid = blkid;
        this.slotNumber = slotNumber;
    }

    public RecordID(byte[] b){
        Page p = new Page(b);
        p.position(0);

        String filename = p.getString();
        int blockNumber = p.getInt();
        int slotNumber = p.getInt();

        this.blkid = new BlockID(filename, blockNumber);
        this.slotNumber = slotNumber;
    }

    public BlockID getBlockID(){
        return blkid;
    }

    public int getSlotNumber(){
        return slotNumber;
    }

    public void setSlotNumber(int slotNumber){
        this.slotNumber = slotNumber;
    }

    public void setBlockID(BlockID blkid){
        this.blkid = blkid;
    }
    // too long table names not supported
    public byte[] toByteArray(){
        Page p = new Page(SIZE);
        p.position(0);
        p.setString(blkid.getFileName());
        p.setInt(blkid.getBlockNumber());
        p.setInt(slotNumber);
        return p.getByteBuffer().array();
    }



}
