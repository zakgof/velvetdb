package com.zakgof.db.kvs;

public interface IByteArrayKvs {

    public byte[] get(byte[] keyBytes);

    public void put(byte[] keyBytes, byte[] valueBytes);

    public void delete(byte[] keyBytes);
}
