package org.foxdb.transaction;

import org.apache.commons.io.FileUtils;
import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.concurrency.ConcurrencyManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.SlottedPage;
import org.foxdb.log.LogManager;
import org.foxdb.recovery.RecoveryManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TransactionTest {

    @Test
    public void TestSingleThreadTransaction(){

        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm);
        ConcurrencyManager cm = new ConcurrencyManager();

        Transaction tx1 = new Transaction(cm, rm, bm, 1);
        BlockID blk10 = new BlockID("./testdb/testfile", 10);

        tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));

        tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));

        tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));

        tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));

        tx1.commit();

        iterateBlock(blk10, bm,rm);


        Transaction tx2 = new Transaction(cm, rm, bm, 2);
        tx2.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        tx2.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
        tx2.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
        tx2.remove(blk10, 1);
        tx2.rollback();

        iterateBlock(blk10, bm,rm);


//        iterateLogs(lm,rm);



        /* only varadrajan left */






        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void iterateLogs(LogManager lm, RecoveryManager rm){
        var logIter = lm.iterator();
        while(logIter.hasNext()){
            System.out.println(rm.recString(logIter.next()));
        }

        System.out.println("*******************************");
        System.out.println("*******************************");
    }

    private void iterateBlock(BlockID blk10, BufferManager bm, RecoveryManager rm){
        Buffer buff = bm.pin(blk10);
        SlottedPage sp =new SlottedPage(buff.contents());
        for(int i=0;i<sp.length();i++){
            String s = new String(sp.get(i), StandardCharsets.UTF_8);
            System.out.println(s);
        }
    }

}
