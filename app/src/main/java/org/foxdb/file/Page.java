package org.foxdb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer bb;
    private final Charset CHARSET = StandardCharsets.UTF_8;

    public Page(int blockSize){
        bb = ByteBuffer.allocate(blockSize);
    }
    public Page(byte[] b){
        bb = ByteBuffer.wrap(b);
    }

    public int getInt(int offSet){
        return bb.getInt(offSet);
    }

    public void setInt(int offSet, int val){
        bb.putInt(offSet, val);
    }

    public String getString(int offSet){
        byte[] b = getBytes(offSet);
        return new String(b, CHARSET);
    }
    public void setString(int offSet, String value){
        byte[] b = value.getBytes(CHARSET);
        setBytes(offSet, b);
    }

    public byte[] getBytes(int offSet){
        bb.position(offSet);
        int length = bb.getInt();

        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }


    public void setBytes(int offSet, byte[] b){
        bb.position(offSet);
        bb.putInt(b.length);
        bb.put(b);
    }

    protected ByteBuffer getByteBuffer(){
        return bb;
    }
}
