package com.zakgof.db.cache;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GuavaCache implements ICache {

    private Cache<Object, Object> cache;

    public GuavaCache(long maxSize, long minutes) {
        cache = CacheBuilder.newBuilder().maximumWeight(maxSize).weigher((k, v) -> weight(k, v)).expireAfterWrite(minutes, TimeUnit.MINUTES).build();
    }

    private int weight(Object k, Object v) {
        return weight(k) + weight(v);
    }

    private int weight(Object v) {
        if (v instanceof byte[])
            return ((byte[]) v).length;
        else if (v instanceof String)
            return ((String) v).length() * 2;
        else if (v.getClass().isArray()) {
            Object[] arr = (Object[]) v;
            int size = 0;
            for (Object o : arr)
                size += weight(o);
            return size;
        } else {
            return 128;
        }

    }

    @Override
    public Object get(Object key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void remove(Object key) {
        cache.invalidate(key);
    }

    @Override
    public void put(Object key, Object value) {
        cache.put(key, value);
    }

}
