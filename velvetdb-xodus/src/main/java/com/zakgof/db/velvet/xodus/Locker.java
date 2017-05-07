package com.zakgof.db.velvet.xodus;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import com.zakgof.db.velvet.VelvetException;

public class Locker {

    private Set<String> pool = new HashSet<>();

    public synchronized void lock(String name) {
        while (pool.contains(name)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new VelvetException(e);
            }
        }
        pool.add(name);
    }

    public synchronized void unlock(String name) {
        pool.remove(name);
        notifyAll();
    }

    public <R> R with(String name, Callable<R> callable) {
        try {
            lock(name);
            return callable.call();
        } catch (Exception e) {
            throw new VelvetException(e);
        } finally {
            unlock(name);
        }
    }
}
