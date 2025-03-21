package org.foxdb.concurrency;

import org.foxdb.file.BlockID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrencyManager {

    private Map<BlockID, List<Integer>> sLocks = new HashMap();
    private Map<BlockID, Integer> xLocks = new HashMap();

    public synchronized void sLock(BlockID blk, int txNum){

        if(sLocks.containsKey(blk) && sLocks.get(blk).contains(txNum)) return; // if current txn already have Slock, no need to take again
        while(xLocks.containsKey(blk)){
            if(xLocks.get(blk) == txNum) break; // if current txn has xLock , let it also have sLock
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        List<Integer> lockHolder = sLocks.getOrDefault(blk, new ArrayList());
        if(!lockHolder.contains(txNum)) lockHolder.add(txNum);
        sLocks.put(blk, lockHolder);
    }

    public synchronized void xLock(BlockID blk, int txNum){

        if(xLocks.containsKey(blk) && xLocks.get(blk) == txNum) return; // if current txn already has Xlock, no need to take again
        while(sLocks.containsKey(blk) || xLocks.containsKey(blk)){
            if(!xLocks.containsKey(blk) && sLocks.containsKey(blk) && sLocks.get(blk).contains(txNum)) break; // if current txn already has an sLock and noOne has xLock , take xLock
            try{
                wait();
            } catch(InterruptedException e){
                throw new RuntimeException(e);
            }
        }

        xLocks.put(blk, txNum);
    }


    public synchronized void unlock(BlockID blk, int txNum){
        xLocks.remove(blk);
        if(sLocks.containsKey(blk)){
            List<Integer> lockHolders = sLocks.get(blk);
            if(lockHolders.contains(txNum)){
               int index = lockHolders.indexOf(txNum);
               lockHolders.remove(index);
            }

            if(lockHolders.size() == 0){
                sLocks.remove(blk);
            }
        }

        notifyAll();
    }

//    public synchronized void waitOnXLock(BlockID blk, int txNum){
//
//        while(xLocks.containsKey(blk)){
//            try{
//                wait();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

}
