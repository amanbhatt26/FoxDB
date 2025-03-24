package org.foxdb.buffer;

import org.apache.commons.io.FileUtils;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class BufferTest {

    @Test
    public void BufferTesting(){
        FileManager fm = new FileManager(new File("./testdb"), 400);
        LogManager lm = new LogManager(fm, "./testdb/log.file");

        Page p = new Page(400);
        p.setInt(22, 49);
        Buffer bf = new Buffer(fm , lm);

        try {
            fm.appendBlock("./testdb/testFile");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BlockID blkid = new BlockID("./testdb/testFile", 0);
        try{
            fm.write(blkid, p);

        }catch(IOException e){
            throw new RuntimeException();
        }


        bf.assignToBlock(blkid);
        bf.pin();

        assert(bf.isPinned());
        bf.pin();
        bf.unpin();
        bf.unpin();
        assert(!bf.isPinned());
        bf.flush();


        Buffer b2 = new Buffer(fm, lm);
        b2.assignToBlock(blkid);
        assert(b2.contents().getInt(22) == 49);

        try{

        }finally{
            try {
                FileUtils.deleteDirectory(new File("./testdb"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
