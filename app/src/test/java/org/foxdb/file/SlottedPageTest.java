package org.foxdb.file;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SlottedPageTest {

    @Test
    public void SlottedPageTest(){

        FileManager fm = new FileManager(new File("./testdb"), 400);


        SlottedPage sp = new SlottedPage(fm.getBlockSize());

        for(int i=0;i<22;i++){

            String rec = "record " + i;
            byte[] recBytes = rec.getBytes(StandardCharsets.UTF_8);
            if(sp.canInsert(recBytes)){
                sp.put(i, recBytes);
            }
        }


        {
            String rec = "record 22";
            byte[] recBytes = rec.getBytes(StandardCharsets.UTF_8);

            if (sp.canInsert(recBytes)) {
                sp.put(5, recBytes);
            }

            sp.remove(7);
            sp.remove(10);

            if (sp.canInsert(recBytes)) {
                sp.put(7, recBytes);
            }

            sp.defragment();
            if (sp.canInsert(recBytes)) {
                sp.put(10, recBytes);
            }
        }



        try {
            fm.appendBlock("./testdb/testfile");
            fm.write(new BlockID("./testdb/testfile", 0), sp.getPage());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Page newPage = new Page(fm.getBlockSize());
        try {
            fm.read(new BlockID("./testdb/testfile", 0), newPage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SlottedPage spnew = new SlottedPage(newPage);


        for(int i=0;i<spnew.length();i++){
            byte[] recBytes = spnew.get(i);
            String rec = recBytes == null ? "null"  : new String(recBytes, StandardCharsets.UTF_8);
            System.out.println(rec);
        }


        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
