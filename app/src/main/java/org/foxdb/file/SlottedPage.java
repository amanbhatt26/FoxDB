package org.foxdb.file;

public class SlottedPage {

    private Page p;
    public SlottedPage(Page p){
        this.p = p;
    }

    public SlottedPage(int blockSize){
        this.p = new Page(blockSize);
        this.p.setInt(0, 0); // length of the slot array
        this.p.setInt(Integer.BYTES, capacity()); // offset of the closest stored element

    }

    public SlottedPage(byte[] b){
        this.p = new Page(b);
    }

    public int length(){
        return this.p.getInt(0);
    }

    public int capacity(){
        return p.capacity();
    }

    public boolean canInsert(byte[] b){
        int bytesNeeded = b.length + Integer.BYTES;
        p.position(0);

        int length = p.getInt();
        int lastOffset = p.getInt();

        int metadataBytes = (2 + length())* Integer.BYTES;
        int newIndexSlotBytes = Integer.BYTES;
        return lastOffset - bytesNeeded >= metadataBytes + newIndexSlotBytes;
    }

    /* will insert the byte array into the desired index inside the slotted page assuming it can be inserted */
    public void put(int index, byte[] b){

        int bytesNeeded = b.length + Integer.BYTES;
        p.position(0);

        int length = p.getInt();
        int lastOffset = p.getInt();


        // insert byte array into the page
        int offset = lastOffset - bytesNeeded;
        p.setBytes(offset, b);

        if(index >= length){
            int indexOffset = (length + 2)*(Integer.BYTES);
            p.setInt(indexOffset, offset);


        }else{
            int i;
            for(i=length-1; i>=0 && i >= index; i--){
                int curIndexOffset = (2+i)*(Integer.BYTES);
                int nextIndexOffset = curIndexOffset + Integer.BYTES;
                p.setInt(nextIndexOffset, p.getInt(curIndexOffset));
            }

            int curIndexOffset = (2+i)*(Integer.BYTES);
            int nextIndexOffset = curIndexOffset + Integer.BYTES;
            p.setInt(nextIndexOffset, offset);

        }

        /* update slot array metadata */
        p.setInt(0 ,length+1);
        p.setInt(Integer.BYTES, offset);

    }

    public void remove(int index){
        if(index < 0 || index >= length()) return;
        int length = length();
        int i;
        for(i=index;i<length-1;i++){
            int curIndexOffset = (2+i)*(Integer.BYTES);
            int nextIndexOffset = curIndexOffset + Integer.BYTES;
            p.setInt(curIndexOffset, p.getInt(nextIndexOffset));
        }

        p.setInt(0, length-1);
    }

    public byte[] get(int index){
        int offset = (index+2)*(Integer.BYTES);
        int valOffset = p.getInt(offset);
        return p.getBytes(valOffset);
    }

    public void defragment(){
        SlottedPage spNew = new SlottedPage(this.capacity());

        int length =  length();
        for(int i=0;i<length;i++){
            spNew.put(i, this.get(i));
        }

        this.p = spNew.getPage();
    }

    public Page getPage(){
        return this.p;
    }
}
