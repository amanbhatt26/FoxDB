package org.foxdb.log;

import org.apache.commons.io.FileUtils;
import org.foxdb.file.FileManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class LogTest {

    @Test
    public void LogTesting() {

        FileManager fm = new FileManager(new File("./testdb"), 400);
        LogManager lm = new LogManager(fm, "./testdb/test.log");

        for(int i=0;i<=4;i++){
            String log = "record:" + i;
            byte[] rec = log.getBytes(StandardCharsets.UTF_8);
            lm.append(rec);
        }


        var iter  = lm.iterator();
        try{
            int i = 4;
            while (iter.hasNext()) {
                byte[] rec = iter.next();
                String readRec = new String(rec, StandardCharsets.UTF_8);

                if(!readRec.equals("record:" + i)){
                    throw new AssertionError();
                }
                i--;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            try {
                FileUtils.deleteDirectory(new File("./testdb"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }




}
