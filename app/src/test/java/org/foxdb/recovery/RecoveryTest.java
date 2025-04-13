package org.foxdb.recovery;

import org.apache.commons.io.FileUtils;
import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.concurrency.ConcurrencyManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.file.SlottedPage;
import org.foxdb.log.LogIterator;
import org.foxdb.log.LogManager;
import org.foxdb.transaction.Transaction;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecoveryTest {

    @Test
    public void TestRecoveryLogging(){


        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 3);
        RecoveryManager rm = new RecoveryManager(lm , bm, fm);

        try {
            fm.appendBlock("./testdb/testfile");
            fm.appendBlock("./testdb/testfile");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        BlockID blk20 =  new BlockID("./testdb/testfile", 0);
        rm.logStart(1);
        rm.logInsert(1, blk20, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        rm.logUpdate(1, blk20,0, "Aman".getBytes(StandardCharsets.UTF_8), "Bhatt".getBytes(StandardCharsets.UTF_8));
        rm.logRemove(1, blk20, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        lm.flush(rm.logCommit(1));


        BlockID blk22 = new BlockID("./testdb/testfile", 1);
        rm.logStart(2);
        rm.logInsert(2, blk22, 0, "Bhatt".getBytes(StandardCharsets.UTF_8));
        rm.logUpdate(2,blk22, 0, "Bhatt".getBytes(StandardCharsets.UTF_8), "Aman".getBytes(StandardCharsets.UTF_8));
        rm.logRemove(2, blk22, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        rm.rollback(2);


//        Buffer buff = bm.pin(new BlockID("./testdb/testfile", 22));
//        Page bp = buff.contents();
//        SlottedPage sp = new SlottedPage(bp);
//        byte[] bytes = sp.get(0);
//        String recString = new String(bytes, StandardCharsets.UTF_8);
//        assert("Bhatt".equals(recString));

        var iter = lm.iterator();
        while(iter.hasNext()){
          byte[] b=  iter.next();
            String recString = rm.recString(b);
//            System.out.println(recString);
        }

        while(iter.hasPrevious()){
            byte[] b = iter.previous();
            String recString = rm.recString(b);
//            System.out.println(recString);
        }

        while(iter.hasNext()){
            byte[] b=  iter.next();
            String recString = rm.recString(b);
            System.out.println(recString);
        }

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void TestRecoveryLogic(){
        BlockID blk10 = new BlockID("./testdb/testfile", 0);
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();

        try {
            fm.appendBlock("./testdb/testfile");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Thread t = new Thread(()->{
            Transaction tx1 = new Transaction(cm, rm, bm, fm,1);
            tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));
            tx1.commit();
        });

        Thread t2 = new Thread(()->{
            Transaction tx2 = new Transaction(cm, rm, bm, fm,2);
            tx2.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx2.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx2.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx2.remove(blk10, 1);
            tx2.commit();

        });

        t.start();
        t2.start();

        try {
            t.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Before Crash *************************************");
        List<String> beforeCrash = iterateBlock(blk10, bm, rm);


        System.out.println("After Crash ***************************************");

        BufferManager bm2 = new BufferManager(fm, lm, 10);
        iterateBlock(blk10, bm2, rm);


        System.out.println("After recovery *************************************");
        LogManager lm2 = new LogManager(fm, "./testdb/test.log");
        RecoveryManager rm2 = new RecoveryManager(lm2, bm2, fm);
        rm2.doRecovery();
        List<String> afterRecovery = iterateBlock(blk10, bm2, rm2);

        assert(beforeCrash.size() == afterRecovery.size());

        for(int i=0;i<beforeCrash.size();i++){
            assert(beforeCrash.get(i).equals(afterRecovery.get(i)));
        }
        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private List<String> iterateBlock(BlockID blk10, BufferManager bm, RecoveryManager rm){
        List<String> elements = new ArrayList();
        Buffer buff = bm.pin(blk10);
        SlottedPage sp =new SlottedPage(buff.contents());
        for(int i=0;i<sp.length();i++){
            byte[] recBytes = sp.get(i);
            String s = recBytes==null ? "null": new String(recBytes, StandardCharsets.UTF_8);
            System.out.println(s);
            elements.add(s);
        }

        return elements;
    }

}
