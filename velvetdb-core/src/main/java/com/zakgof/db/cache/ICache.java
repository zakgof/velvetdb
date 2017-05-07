package com.zakgof.db.cache;

public interface ICache {

    Object get(Object key);

    void remove(Object key);

    void put(Object key, Object value);

}
