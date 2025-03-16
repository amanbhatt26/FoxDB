package org.foxdb.buffer;

import org.apache.commons.io.FileUtils;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class BufferManagerTest {

    @Test
    public void BufferManagerTesting(){
        FileManager fm = new FileManager(new File("./testdb"), 500);
        LogManager lm = new LogManager(fm, "./testdb/test.log");
        BufferManager bm = new BufferManager(fm, lm, 3);

        Page p = new Page(400);
        p.setInt(20, 100);

        BlockID blkid = new BlockID("./testdb/TestFile", 20);
        try{
            fm.write(blkid, p);
        }catch(IOException e){
            e.printStackTrace();
            throw new RuntimeException();
        }

        Buffer buff = bm.pin(blkid);
        assert(bm.numUnpinnedBuffers() == 2);

        Page newPage = buff.contents();

        assert(newPage.getInt(20) == 100);

        Buffer buff2 = bm.pin(blkid);
        assert(bm.numUnpinnedBuffers() == 2);

        buff.unpin();

        bm.pin(new BlockID("./testdb/TestFile", 22));
        bm.pin(new BlockID("./testdb/TestFile", 25));

        try{
            bm.pin(new BlockID("TestFile", 26));
        }catch(RuntimeException e){
            assert(true);
        }catch(Exception e){
            assert(false);
        }finally{
            try {
                FileUtils.deleteDirectory(new File("./testdb"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
