package org.foxdb.concurrency;

import org.foxdb.file.BlockID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrencyManager {

    private final Map<BlockID, List<Integer>> sLocks = new HashMap<>();
    private final Map<BlockID, Integer> xLocks = new HashMap<>();

    private final Map<String, List<Integer>> sLocksEOF = new HashMap<>();
    private final Map<String, Integer> xLocksEOF = new HashMap<>();

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
        List<Integer> lockHolder = sLocks.getOrDefault(blk, new ArrayList<Integer>());
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
                lockHolders.remove((Integer) txNum);
            }

            if(lockHolders.isEmpty()){
                sLocks.remove(blk);
            }
        }

        notifyAll();
    }

    public synchronized void sLock(String fileName, int txNum){

        if(sLocksEOF.containsKey(fileName) && sLocksEOF.get(fileName).contains(txNum)) return; // if current txn already have Slock, no need to take again
        while(xLocksEOF.containsKey(fileName)){
            if(xLocksEOF.get(fileName) == txNum) break; // if current txn has xLock , let it also have sLock
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        List<Integer> lockHolder = sLocksEOF.getOrDefault(fileName, new ArrayList<Integer>());
        if(!lockHolder.contains(txNum)) lockHolder.add(txNum);
        sLocksEOF.put(fileName, lockHolder);
    }

    public synchronized void xLock(String blk, int txNum){

        if(xLocksEOF.containsKey(blk) && xLocksEOF.get(blk) == txNum) return; // if current txn already has Xlock, no need to take again
        while(sLocksEOF.containsKey(blk) || xLocksEOF.containsKey(blk)){
            if(!xLocksEOF.containsKey(blk) && sLocksEOF.containsKey(blk) && sLocksEOF.get(blk).contains(txNum)) break; // if current txn already has an sLock and noOne has xLock , take xLock
            try{
                wait();
            } catch(InterruptedException e){
                throw new RuntimeException(e);
            }
        }

        xLocksEOF.put(blk, txNum);
    }


    public synchronized void unlock(String blk, int txNum){
        xLocksEOF.remove(blk);
        if(sLocksEOF.containsKey(blk)){
            List<Integer> lockHolders = sLocksEOF.get(blk);
            if(lockHolders.contains(txNum)){
                lockHolders.remove((Integer) txNum);
            }

            if(lockHolders.isEmpty()){
                sLocksEOF.remove(blk);
            }
        }
        notifyAll();
    }
}
