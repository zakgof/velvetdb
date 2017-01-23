package com.zakgof.db.kvs;

public interface IKvs {

    public <T> T get(Class<T> clazz, Object key);

    public <T> void put(Object key, T value);

    public void delete(Object key);

}
