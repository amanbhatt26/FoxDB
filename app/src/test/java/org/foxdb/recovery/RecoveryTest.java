package org.foxdb.recovery;

import org.apache.commons.io.FileUtils;
import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class RecoveryTest {

    @Test
    public void TestRecoveryLogging(){


        FileManager fm = new FileManager(new File("./testdb"), 4000);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 3);
        RecoveryManager rm = new RecoveryManager(lm , bm);

        rm.logStart(1);
        rm.logUpdate(1, new BlockID("./testdb/testfile", 20), 20, "Aman", "Bhatt");
        rm.logUpdate(1, new BlockID("./testdb/testfile", 50), 22, 20,21);

        lm.flush(rm.logCommit(1));

        rm.logStart(2);
        rm.logUpdate(2, new BlockID("./testdb/testfile", 22), 20, "Bhatt", "Aman");

        rm.rollback(2);


        Buffer buff = bm.pin(new BlockID("./testdb/testfile", 22));
        Page bp = buff.contents();

        assert("Bhatt".equals(bp.getString(20)));

        Iterator<byte[]> iter = lm.iterator();
        int count = 0;

        while(iter.hasNext()){
            count++;
            byte[] b = iter.next();
            System.out.println(rm.recString(b));
        }

        assert(count == 7);

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
