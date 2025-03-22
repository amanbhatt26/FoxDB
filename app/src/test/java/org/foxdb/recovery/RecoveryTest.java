package org.foxdb.recovery;

import org.apache.commons.io.FileUtils;
import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.file.SlottedPage;
import org.foxdb.log.LogIterator;
import org.foxdb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class RecoveryTest {

    @Test
    public void TestRecoveryLogging(){


        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 3);
        RecoveryManager rm = new RecoveryManager(lm , bm, fm);

        BlockID blk20 =  new BlockID("./testdb/testfile", 20);
        rm.logStart(1);
        rm.logInsert(1, blk20, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        rm.logUpdate(1, blk20,0, "Aman".getBytes(StandardCharsets.UTF_8), "Bhatt".getBytes(StandardCharsets.UTF_8));
        rm.logRemove(1, blk20, 0, "Aman".getBytes(StandardCharsets.UTF_8));
        lm.flush(rm.logCommit(1));


        BlockID blk22 = new BlockID("./testdb/testfile", 22);
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
        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 3);
        RecoveryManager rm = new RecoveryManager(lm , bm, fm);

        Buffer buff = bm.pin(new BlockID("./testdb/testfile", 0));
        SlottedPage sp = new SlottedPage(buff.contents());
        sp.defragment();

        buff.setModified(0, 0);
        bm.flushAll(0);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
