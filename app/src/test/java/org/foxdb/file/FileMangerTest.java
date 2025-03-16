package org.foxdb.file;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class FileMangerTest {

    @Test
    public void CreateDB(){
        FileManager fm = new FileManager(new File("./testdb"), 400);
        assert(fm!=null);

    }

    @Test
    public void WriteToFile(){
        FileManager fm = new FileManager(new File("./testdb"), 400);
        try{
            Page p = new Page(400);
            p.setString(20, "Aman Bhatt");
            fm.write(new BlockID("./testdb/testtable.tbl", 27), p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Test
    public void ReadFromFile(){
        FileManager fm = new FileManager(new File("./testdb"), 400);
        Page p = new Page(400);
        try {
            fm.read(new BlockID("./testdb/testtable.tbl", 27), p);
            String str = p.getString(20);
            assert(str.equals("Aman Bhatt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            FileUtils.deleteDirectory(new File("./testdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
