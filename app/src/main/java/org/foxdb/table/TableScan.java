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
    private String stringFilename;

    public TableScan(Schema schema,Transaction transaction, String filename, String stringFilename) {
        this.schema = schema;
        this.layout = new Layout(schema);
        this.transaction = transaction;
        this.filename = filename;
        this.currentRecord = new RecordID(new BlockID(filename, 0), 0);
        this.stringFilename = stringFilename;
    }


    public void beforeFirst(){
        currentRecord = new RecordID(new BlockID(filename, 0), -1);
    }

    public boolean hasNext(){
        int blockNumber = currentRecord.getBlockID().getBlockNumber();
        int slotNumber = currentRecord.getSlotNumber();

        return !(blockNumber >= transaction.fileLength(filename)-1 || (blockNumber == transaction.fileLength(filename)-1 &&
                slotNumber >= transaction.slotArrayLength(currentRecord.getBlockID())-1));
    }

    public Record next(){
        if(currentRecord.getSlotNumber() < transaction.slotArrayLength(currentRecord.getBlockID())-1){
            currentRecord.setSlotNumber(currentRecord.getSlotNumber() + 1);
            return loadRecord(currentRecord);
        }

        currentRecord.setBlockID(
                new BlockID(
                        currentRecord.getBlockID().getFileName(),
                        currentRecord.getBlockID().getBlockNumber()+1
                )
        );

        return loadRecord(currentRecord);
    }


    public void insert(Record record){

        if (!check(record)){
            throw new RuntimeException("Schema mismatch");
        }

        RecordID emptySlot = emptyOrNewSlot();


        var fields = schema.fields();
        byte[] recordBytes = new byte[layout.recordSize()];
        ByteBuffer recordByteBuffer = ByteBuffer.wrap(recordBytes);

        for(var f:fields){
            int offset = layout.offset(f);
            int size = layout.size(f);
            Schema.Type type = schema.type(f);

            if(type == Schema.Type.INTEGER){
                recordByteBuffer.putInt(offset,record.readInt(f));
            }else if(type == Schema.Type.STRING){

                RecordID stringRecID = saveString(record.readString(f));
                byte[] stringRecIDBytes = stringRecID.toByteArray();
                recordByteBuffer.put(offset,stringRecIDBytes);
            }
        }

        transaction.update(emptySlot.getBlockID(), emptySlot.getSlotNumber(), recordBytes);
    }

    public RecordID saveString(String value){

        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        int stringFileLength = transaction.fileLength(stringFilename);
        if(stringFileLength == 0){
            transaction.appendNewBlock(stringFilename);
        }
        BlockID blockCursor = new BlockID(stringFilename, 0);
        while(blockCursor.getBlockNumber() < stringFileLength && ! transaction.fitCheck(blockCursor, valueBytes)){
            blockCursor = new BlockID(blockCursor.getFileName(), blockCursor.getBlockNumber()+1);
        }

        if(transaction.fitCheck(blockCursor, valueBytes)){
            int slotNumber = transaction.slotArrayLength(blockCursor);
            transaction.insert(blockCursor, slotNumber, valueBytes);
            return new RecordID(blockCursor, slotNumber);
        }

        BlockID newBlock = transaction.appendNewBlock(stringFilename);
        transaction.insert(newBlock, 0, valueBytes);
        return new RecordID(newBlock, 0);
    }

    public RecordID emptyOrNewSlot(){
        // TODO: optimise
        RecordID bookMark = new RecordID(currentRecord);

        while(hasNext()){

            Record potentialSlot = next();
            if(potentialSlot == null){
                RecordID empty = new RecordID(currentRecord);
                currentRecord = bookMark; // correct the tablescan value
                return empty;
            }
        }

        currentRecord = bookMark;
        BlockID newBlock = transaction.appendNewBlock(filename);
        transaction.insert(newBlock, 0, new byte[0]);
        return new RecordID(newBlock,0);
    }


    public boolean check(Record record){
        return record.getSchema().equals(schema);
    }

    private Record loadRecord(RecordID rid){

        // Make record from byte array

        byte[] recordBytes = transaction.read(rid.getBlockID(), rid.getSlotNumber());
        if(recordBytes == null) return null;
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
                b.get(offset,p);
                RecordID stringRecordID = new RecordID(p);
                byte[] stringBytes = transaction.read(stringRecordID.getBlockID(), stringRecordID.getSlotNumber());
                String val = new String(stringBytes, StandardCharsets.UTF_8);
                record.updateString(f,val);
            }
        }

        return record;
    }

}
