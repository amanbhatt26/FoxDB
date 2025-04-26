package org.foxdb.table;

import org.apache.commons.io.FileUtils;
import org.foxdb.buffer.BufferManager;
import org.foxdb.concurrency.ConcurrencyManager;
import org.foxdb.file.FileManager;
import org.foxdb.log.LogManager;
import org.foxdb.recovery.RecoveryManager;
import org.foxdb.transaction.Transaction;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class TableScanTest {
    @Test
    public void TestTableScan(){
        FileManager fm = new FileManager(new File("./testdb"), 4000);

        ConcurrencyManager cm = new ConcurrencyManager();
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 50);
        RecoveryManager rm = new RecoveryManager(lm,bm,fm);
        Transaction transaction = new Transaction(cm, rm,bm,fm,1);

        Schema schema = new Schema();
        schema.add("Name", Schema.Type.STRING);
        schema.add("Age", Schema.Type.INTEGER);
        schema.add("Phone", Schema.Type.STRING);
        schema.add("JobID", Schema.Type.STRING);
        TableScan tblscn = new TableScan(schema, transaction, "./testdb/dbfile", "./testdb/dbstringfile");
        tblscn.beforeFirst();
        Record firstRecord = new Record(null, schema);
        firstRecord.updateString("JobID", "Software Engineer");
        firstRecord.updateString("Name", "Aman Bhatt");
        firstRecord.updateString("Phone", "8200277151");
        firstRecord.updateInt("Age", 23);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);
        tblscn.insert(firstRecord);

        transaction.commit();
        bm.flushAll(1);

        ReadTableScanTest();

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void ReadTableScanTest(){
        FileManager fm = new FileManager(new File("./testdb"), 4000);

        ConcurrencyManager cm = new ConcurrencyManager();
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 50);
        RecoveryManager rm = new RecoveryManager(lm,bm,fm);
        Transaction transaction = new Transaction(cm, rm,bm,fm,1);

        Schema schema = new Schema();
        schema.add("Name", Schema.Type.STRING);
        schema.add("Age", Schema.Type.INTEGER);
        schema.add("Phone", Schema.Type.STRING);
        schema.add("JobID", Schema.Type.STRING);
        TableScan tblscn = new TableScan(schema, transaction, "./testdb/dbfile", "./testdb/dbstringfile");
        tblscn.beforeFirst();
        for(var f:schema.fields()){
            System.out.print(f + " | ");
        }
        System.out.println("");
        while(tblscn.hasNext()){
            Record record  = tblscn.next();
            for(var f:schema.fields()){
                Schema.Type type = schema.type(f);
                System.out.print( (type== Schema.Type.INTEGER ? record.readInt(f) : record.readString(f)) + " | ");
            }

            System.out.println("");
        }
    }
}
