package com.zakgof.db.sqlkvs;

import txn.ITransactionalEnvironment;

public interface IByteArrayKvs extends ITransactionalEnvironment {

  public byte[] get(byte[] keyBytes);

  public void put(byte[] keyBytes, byte[] valueBytes);

  public void delete(byte[] keyBytes);
}
