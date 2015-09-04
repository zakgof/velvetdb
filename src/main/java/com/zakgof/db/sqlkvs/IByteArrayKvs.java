package com.zakgof.db.sqlkvs;

public interface IByteArrayKvs {

  public byte[] get(byte[] keyBytes);

  public void put(byte[] keyBytes, byte[] valueBytes);

  public void delete(byte[] keyBytes);
}
