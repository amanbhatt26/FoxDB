package org.foxdb.table;

import org.foxdb.file.BlockID;
import org.foxdb.file.Page;
import org.foxdb.transaction.Transaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TableScan {

    private Schema schema;
    private Layout layout;
    private RecordID currentRecord;
    private Transaction transaction;
    private String filename;

    public TableScan(Schema schema,Transaction transaction, String filename) {
        this.schema = schema;
        this.layout = new Layout(schema);
        this.transaction = transaction;
        this.filename = filename;
        this.currentRecord = new RecordID(new BlockID(filename, 0), 0);
    }


    public void first(){
        currentRecord = new RecordID(new BlockID(filename, 0), 0);
    }

    public boolean hasNext(){
        int blockNumber = currentRecord.getBlockID().getBlockNumber();
        int slotNumber = currentRecord.getSlotNumber();

        return blockNumber < transaction.fileLength(filename)-1 ||
                slotNumber < transaction.slotArrayLength(currentRecord.getBlockID())-1;
    }

    public void next(){
        if(currentRecord.getSlotNumber() < transaction.slotArrayLength(currentRecord.getBlockID())-1){
            currentRecord.setSlotNumber(currentRecord.getSlotNumber() + 1);
            return;
        }

        currentRecord.setBlockID(
                new BlockID(
                        currentRecord.getBlockID().getFileName(),
                        currentRecord.getBlockID().getBlockNumber()+1
                )
        );
    }

    public Record current(){
        return loadRecord(currentRecord);
    }

    public void insert(Record record){
        // TODO: find a block with an empty slot or enough space or create a new block and insert the record
    }

    private Record loadRecord(RecordID rid){
        byte[] recordBytes = transaction.read(rid.getBlockID(), rid.getSlotNumber());
        ByteBuffer b = ByteBuffer.wrap(recordBytes);
        Record record = new Record(rid, schema);
        var fields = schema.fields();
        for(var f:fields){
            int offset = layout.offset(f);
            int size = layout.size(f);
            if(schema.type(f) == Schema.Type.INTEGER){
                record.updateInt(f,b.getInt(offset));
            }else if(schema.type(f) == Schema.Type.STRING){
                // load string
                byte[] p = new byte[RecordID.SIZE];
                b.get(p, offset,size);
                RecordID stringRecordID = new RecordID(p);
                byte[] stringBytes = transaction.read(stringRecordID.getBlockID(), stringRecordID.getSlotNumber());
                String val = new String(stringBytes, StandardCharsets.UTF_8);
                record.updateString(f,val);
            }
        }

        return record;
    }

}
