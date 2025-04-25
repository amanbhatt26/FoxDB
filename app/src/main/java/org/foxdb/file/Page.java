package org.foxdb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer bb;
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public int capacity(){
        return bb.capacity();
    }
    public void position(int offset){
        this.bb.position(offset);
    }

    public void setInt(int val){
        bb.putInt(val);
    }

    public void setString(String val){
        setString(bb.position(), val);
    }

    public int getInt(){
        return bb.getInt();
    }

    public String getString(){
        return getString(bb.position());
    }

    public Page(int blockSize){
        bb = ByteBuffer.allocate(blockSize);
        position(0);
    }
    public Page(byte[] b){
        bb = ByteBuffer.wrap(b);
        position(0);
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

    public byte[] getBytes(){
        int position = bb.position();
        byte[] b = getBytes(position);
        position(position + Integer.BYTES + b.length);
        return b;
    }

    public void setBytes(byte[] b){
        int position = bb.position();
        setBytes(position, b);
        position(position + Integer.BYTES + b.length);
    }


    public void setBytes(int offSet, byte[] b){
        bb.position(offSet);
        bb.putInt(b.length);
        bb.put(b);
    }

    public ByteBuffer getByteBuffer(){
        return bb;
    }

    public static int stringBytesNeeded(String val){
        return Integer.BYTES + val.getBytes(CHARSET).length;
    }
}
