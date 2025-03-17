package org.foxdb.recovery;

import org.foxdb.buffer.Buffer;
import org.foxdb.buffer.BufferManager;
import org.foxdb.file.BlockID;
import org.foxdb.file.Page;
import org.foxdb.log.LogManager;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
/* Recovery manager is responsible for rollback of transactions and recovery on db startup */
/* RecoveryManager also logs all the log records as the transaction proceeds so it can do recovery and rollbacks */

public class RecoveryManager {
    private LogManager lm;
    private BufferManager bm;
    private enum logType{START, COMMIT,ROLLBACK, UPDATE};
    public RecoveryManager(LogManager lm, BufferManager bm){
        this.lm = lm;
        this.bm = bm;
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
        p.setInt(logType.ROLLBACK.ordinal());
        p.setInt(txId);
        return lm.append(b);
    }

    public int logUpdate(int txId, BlockID blkId, int offSet, int oldVal, int newVal){

        int bytesNeeded = 8*Integer.BYTES + Page.stringBytesNeeded(blkId.getFileName());
        byte[] b = new byte[bytesNeeded];
        Page p = new Page(b);
        p.setInt(logType.UPDATE.ordinal());
        p.setInt(txId);
        p.setString(blkId.getFileName());
        p.setInt(blkId.getBlockNumber());
        p.setInt(offSet);
        p.setInt(0); // type of value
        p.setInt(oldVal);
        p.setInt(newVal);
        return lm.append(b);
    }

    public int logUpdate(int txId, BlockID blkId, int offSet, String oldVal, String newVal){

        int bytesNeeded = 6*Integer.BYTES + Page.stringBytesNeeded(blkId.getFileName()) + Page.stringBytesNeeded(oldVal) + Page.stringBytesNeeded(newVal);
        byte[] b = new byte[bytesNeeded];
        Page p = new Page(b);
        p.setInt(logType.UPDATE.ordinal());
        p.setInt(txId);
        p.setString(blkId.getFileName());
        p.setInt(blkId.getBlockNumber());
        p.setInt(offSet);
        p.setInt(1); // type of value
        p.setString(oldVal);
        p.setString(newVal);
        return lm.append(b);
    }


    public void rollback(int txId){
        Iterator<byte[]> logIterator = lm.iterator();
        while(logIterator.hasNext()){
            byte[] rec = logIterator.next();
            Page p = new Page(rec);

            p.position(0);
            int lType = p.getInt();

            if(lType == logType.START.ordinal()){
                break;
            }

            int recTxId = p.getInt();
            if(recTxId != txId) continue;
            BlockID blk = new BlockID(p.getString(), p.getInt());
            Buffer buff = bm.pin(blk);
            int offSet = p.getInt();
            int type = p.getInt();
            if(type == 0){
                int oldval = p.getInt();
                int newVal = p.getInt();
                Page contents = buff.contents();
                contents.setInt(offSet, oldval);
            }else{
                String oldVal = p.getString();
                String newVal = p.getString();
                Page contents = buff.contents();
                contents.setString(offSet, oldVal);
            }
            bm.unpin(buff);
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
            case UPDATE:
                txId = p.getInt();
                String filename = p.getString();
                int blockNUm = p.getInt();
                int offSet = p.getInt();
                int tp = p.getInt();
                String recValue = "<UPDATE," + txId +"," + filename + "," + blockNUm + "," + offSet + "," + tp + ",";
                if(tp == 0){
                    int oldVal = p.getInt();
                    int newVal = p.getInt();
                    recValue += oldVal + "," + newVal + ">";

                }else{
                    String oldVal = p.getString();
                    String newVal = p.getString();
                    recValue += oldVal + "," + newVal + ">";
                }

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
}
