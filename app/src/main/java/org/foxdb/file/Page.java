package org.foxdb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer bb;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    int offset = 0;

    public int capacity(){
        return bb.capacity();
    }
    public void position(int offset){
        this.offset = offset;
    }

    public void setInt(int val){
        setInt(this.offset, val);
        this.offset += Integer.BYTES;
    }

    public void setString(String val){
        setString(this.offset, val);
        this.offset += val.getBytes(CHARSET).length + Integer.BYTES;
    }

    public int getInt(){
        int val = getInt(this.offset);
        this.offset += Integer.BYTES;
        return val;
    }

    public String getString(){
        String val = getString(this.offset);
        this.offset += val.getBytes(CHARSET).length + Integer.BYTES;
        return val;
    }



    public Charset getCHARSET() {
        return CHARSET;
    }

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

    public static int stringBytesNeeded(String val){
        return Integer.BYTES + val.getBytes(CHARSET).length;
    }
}
