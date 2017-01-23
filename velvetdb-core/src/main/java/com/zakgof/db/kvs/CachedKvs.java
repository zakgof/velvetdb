package com.zakgof.db.kvs;

import com.zakgof.db.cache.ICache;
import com.zakgof.tools.Buffer;

public class CachedKvs implements IKvs {

    private final IKvs kvs;
    private final ICache cache;

    public CachedKvs(IKvs kvs, ICache cache) {
        this.kvs = kvs;
        this.cache = cache;
    }

    @Override
    public <T> T get(Class<T> clazz, Object key) {
        T cached = clazz.cast(cache.get(convert(key)));
        if (cached != null)
            return cached;

        T value = kvs.get(clazz, key);
        if (value != null)
            cache.put(convert(key), value);

        return value;
    }

    private static Object convert(Object key) {
        if (key instanceof byte[])
            return new Buffer((byte[]) key);
        return key;
    }

    @Override
    public <T> void put(Object key, T value) {
        kvs.put(key, value);
        cache.put(convert(key), value);
    }

    @Override
    public void delete(Object key) {
        kvs.delete(key);
        cache.remove(convert(key));
    }

}
