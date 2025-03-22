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
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();

        Transaction tx1 = new Transaction(cm, rm, bm,fm, 1);
        BlockID blk10 = new BlockID("./testdb/testfile", 10);

        tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));

        tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));

        tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));

        tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));

        tx1.commit();

        iterateBlock(blk10, bm,rm);


        Transaction tx2 = new Transaction(cm, rm, bm,fm, 2);
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


    @Test
    public void TestMultiThreadWritersTransaction(){
        BlockID blk10 = new BlockID("./testdb/testfile", 10);
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();

        Thread t = new Thread(()->{
            Transaction tx1 = new Transaction(cm, rm, bm, fm,1);
            tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            tx1.commit();
        });

        Thread t2 = new Thread(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Transaction tx2 = new Transaction(cm, rm, bm, fm,2);
            tx2.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx2.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx2.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx2.remove(blk10, 1);
            tx2.rollback();

        });

        t.start();
        t2.start();


        try {
            t.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        iterateBlock(blk10, bm,rm);


        iterateLogs(lm,rm);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    public void TestMultiThreadReadWriteTransaction(){
        BlockID blk10 = new BlockID("./testdb/testfile", 10);
        BlockID blk20 = new BlockID("./testdb/testfile", 20);
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();

        Thread t = new Thread(()->{
            Transaction tx1 = new Transaction(cm, rm, bm, fm,1);
            tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk20, 0, tx1.read(blk10, 1));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            tx1.commit();
        });

        Thread t2 = new Thread(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Transaction tx2 = new Transaction(Transaction.Isolation.READ_UNCOMMITTED ,cm, rm, bm, fm,2);
            byte[] b= tx2.read(blk10, 0);
            byte[] b20 = tx2.read(blk20, 0);
            System.out.println(new String(b, StandardCharsets.UTF_8) + " " + new String(b20, StandardCharsets.UTF_8));
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

        iterateBlock(blk10, bm,rm);
        iterateBlock(blk20, bm,rm);


        iterateLogs(lm,rm);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void TestMultiThreadReadersTransaction(){
        BlockID blk10 = new BlockID("./testdb/testfile", 10);
        BlockID blk20 = new BlockID("./testdb/testfile", 20);
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();

        Thread t = new Thread(()->{
            Transaction tx1 = new Transaction(cm, rm, bm,fm, 1);
            tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
            tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));
            tx1.insert(blk20, 0, tx1.read(blk10, 1));
            tx1.commit();
        });

        Thread t2 = new Thread(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Transaction tx2 = new Transaction(cm, rm, bm,fm, 2);
            byte[] b= tx2.read(blk10, 0);
            byte[] b20 = tx2.read(blk20, 0);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(new String(b, StandardCharsets.UTF_8) + " " + new String(b20, StandardCharsets.UTF_8));
            tx2.commit();

        });

        Thread t3 = new Thread(()->{

            Transaction tx2 = new Transaction(cm, rm, bm, fm, 3);
            byte[] b= tx2.read(blk10, 0);
            byte[] b20 = tx2.read(blk20, 0);


            System.out.println(new String(b, StandardCharsets.UTF_8) + " " + new String(b20, StandardCharsets.UTF_8));
            tx2.commit();

        });





        try {
            t.start();
            t.join();

            t2.start();
            t3.start();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        iterateBlock(blk10, bm,rm);
        iterateBlock(blk20, bm,rm);


        iterateLogs(lm,rm);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void SingleThreadEOFTest(){
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 10);
        RecoveryManager rm = new RecoveryManager(lm, bm, fm);
        ConcurrencyManager cm = new ConcurrencyManager();
        BlockID blk10 = new BlockID("./testdb/testfile", 0);
        Transaction tx1 = new Transaction(cm, rm, bm,fm, 1);
        tx1.appendNewBlock("./testdb/testfile");
        tx1.insert(blk10, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        tx1.insert(blk10, 1, "Bhatt".getBytes(StandardCharsets.UTF_8));
        tx1.update(blk10, 0, "Varadrajan".getBytes(StandardCharsets.UTF_8));
        tx1.insert(blk10, 1, "Sunshine".getBytes(StandardCharsets.UTF_8));
        tx1.insert(blk10, 0, tx1.read(blk10, 1));
        tx1.rollback();



        bm.flushAll(1);

        Transaction tx2 = new Transaction(Transaction.Isolation.SERIALIZABLE,cm, rm, bm, fm, 2);
        System.out.println(tx1.fileLength("./testdb/testfile"));
        tx2.commit();

        iterateBlock(blk10, bm, rm);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void MultiThreadFileEOF(){

    }

}
