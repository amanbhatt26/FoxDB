package org.foxdb.recovery;

import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.FileManager;
import org.foxdb.file.Page;
import org.foxdb.file.SlottedPage;
import org.foxdb.log.LogManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
/* Recovery manager is responsible for rollback of transactions and recovery on db startup */
/* RecoveryManager also logs all the log records as the transaction proceeds so it can do recovery and rollbacks */

public class RecoveryManager {
    private LogManager lm;
    private BufferManager bm;
    private FileManager fm;
    private enum logType{START, COMMIT,ROLLBACK, UPDATE, INSERT, REMOVE, APPEND};
    public RecoveryManager(LogManager lm, BufferManager bm, FileManager fm){
        this.lm = lm;
        this.bm = bm;
        this.fm = fm;
    }

    public int logStart(int txId){
        byte[] b = new byte[2*Integer.BYTES];
        Page p = new Page(b);
        p.position(0);
        p.setInt(logType.START.ordinal());
        p.setInt(txId);
        return lm.append(b);
    }

    public int logCommit(int txId){
        byte[] b = new byte[2*Integer.BYTES];
        Page p = new Page(b);
        p.setInt(logType.COMMIT.ordinal());
        p.setInt(txId);
        return lm.append(b);
    }

    private int logRollback(int txId){
        byte[] b = new byte[2*Integer.BYTES];
        Page p = new Page(b);
        p.position(0);
        p.setInt(logType.ROLLBACK.ordinal());
        p.setInt(txId);
        return lm.append(b);
    }

    public int logUpdate(int txId, BlockID blkId, int index, byte[] oldVal, byte[] newVal){
        int bytesNeededByFilename = Page.stringBytesNeeded(blkId.getFileName());
        int bytesNeeded = (Integer.BYTES* 6) + oldVal.length + newVal.length + bytesNeededByFilename;
        byte[] b = new byte[bytesNeeded];
        Page p = new Page(b);
        p.position(0);
        p.setInt(logType.UPDATE.ordinal());
        p.setInt(txId);
        p.setString(blkId.getFileName());
        p.setInt(blkId.getBlockNumber());
        p.setInt(index);
        p.setBytes(oldVal);
        p.setBytes(newVal);
        return lm.append(b);
    }

    public int logInsert(int txId, BlockID blkId, int index, byte[] val){
        int bytesNeededByFilename = Page.stringBytesNeeded(blkId.getFileName());
        int bytesNeeded = (Integer.BYTES* 5) + val.length + bytesNeededByFilename;
        byte[] b = new byte[bytesNeeded];
        Page p = new Page(b);

        p.position(0);
        p.setInt(logType.INSERT.ordinal());
        p.setInt(txId);
        p.setString(blkId.getFileName());
        p.setInt(blkId.getBlockNumber());
        p.setInt(index);
        p.setBytes(val);

        return lm.append(b);
    }

    public int logRemove(int txId, BlockID blkId, int index, byte[] val){
        int bytesNeededByFilename = Page.stringBytesNeeded(blkId.getFileName());
        int bytesNeeded = (Integer.BYTES* 5) + val.length + bytesNeededByFilename;
        byte[] b = new byte[bytesNeeded];
        Page p = new Page(b);

        p.position(0);
        p.setInt(logType.REMOVE.ordinal());
        p.setInt(txId);
        p.setString(blkId.getFileName());
        p.setInt(blkId.getBlockNumber());
        p.setInt(index);
        p.setBytes(val);

        return lm.append(b);
    }
//    public int logAppend(int txId, String fileName){
//        int bytesNeededByFilename = Page.stringBytesNeeded(fileName);
//        int bytesNeeded = (Integer.BYTES*2) + bytesNeededByFilename;
//        byte[] b = new byte[bytesNeeded];
//        Page p = new Page(b);
//
//        p.position(0);
//        p.setInt(logType.APPEND.ordinal());
//        p.setString(fileName);
//
//        return lm.append(b);
//
//    }
    public void commit(int txId){
        lm.flush(logCommit(txId));
    }
    public void rollback(int txId){
        var logIterator = lm.iterator();
        iteratorLoop : while(logIterator.hasNext()){
            byte[] rec = logIterator.next();
            Page p = new Page(rec);

            p.position(0);
            int lType = p.getInt();
            int recTxId = p.getInt();
            if(recTxId != txId) continue;

            logType value = logType.values()[lType];
            switch(value){
                case START:
                case ROLLBACK:
                    break iteratorLoop;
                case INSERT:{
                    BlockID blk = new BlockID(p.getString(), p.getInt());
                    Buffer buff = bm.pin(blk);
                    SlottedPage sp = new SlottedPage(buff.contents());
                    int index = p.getInt();
                    sp.remove(index);
                    bm.unpin(buff);
                    break;
                }
                case REMOVE: {
                    BlockID blk = new BlockID(p.getString(), p.getInt());
                    Buffer buff = bm.pin(blk);
                    SlottedPage sp = new SlottedPage(buff.contents());
                    int index = p.getInt();
                    byte[] val = p.getBytes();
                    sp.defragment();
                    sp.put(index, val);
                    bm.unpin(buff);
                    break;
                }
                case UPDATE: {
                    BlockID blk = new BlockID(p.getString(), p.getInt());
                    Buffer buff = bm.pin(blk);
                    SlottedPage sp = new SlottedPage(buff.contents());
                    int index = p.getInt();
                    byte[] oldVal = p.getBytes();
                    byte[] newVal = p.getBytes();
                    sp.remove(index);
                    sp.defragment();
                    sp.put(index, oldVal);

                    bm.unpin(buff);
                    break;
                }
//                case APPEND: {
//                    String fileName = p.getString();
//                    try {
//                        fm.truncateLastBlock(fileName);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                    break;
//                }
            }


        }


        lm.flush(logRollback(txId));

    }

    public String recString(byte[] b){
        Page p = new Page(b);
        p.position(0);
        logType type = logType.values()[p.getInt()];
        int txId = 0;
        switch(type){
            case START:
                txId = p.getInt();
                return "<START," + txId + ">";

            case INSERT: {
                txId = p.getInt();
                String filename = p.getString();
                int blockNUm = p.getInt();
                int index = p.getInt();
                String recValue = "<INSERT," + txId + "," + filename + "," + blockNUm + "," + index + ",";
                byte[] val = p.getBytes();
                recValue += new String(val, StandardCharsets.UTF_8) + ">";
                return recValue;
            }
            case REMOVE:{
                txId = p.getInt();
                String filename = p.getString();
                int blockNUm = p.getInt();
                int index = p.getInt();
                String recValue = "<REMOVE," + txId + "," + filename + "," + blockNUm + "," + index + ",";
                byte[] val = p.getBytes();
                recValue += new String(val, StandardCharsets.UTF_8) + ">";
                return recValue;
            }

            case UPDATE:
                txId = p.getInt();
                String filename = p.getString();
                int blockNUm = p.getInt();
                int index = p.getInt();
                String recValue = "<UPDATE," + txId +"," + filename + "," + blockNUm + "," + index + ",";
                byte[] oldVal = p.getBytes();
                byte[] newVal = p.getBytes();

                recValue += new String(oldVal, StandardCharsets.UTF_8) + "," + new String(newVal, StandardCharsets.UTF_8) + ">";
                return recValue;

            case ROLLBACK:
                txId = p.getInt();
                return "<ROLLBACK," + txId + ">";

            case COMMIT:
                txId = p.getInt();
                return "<COMMIT," + txId + ">";

        }

        return "";
    }

    public void doRecovery(){

    }
}
