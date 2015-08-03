package com.zakgof.db.sqlkvs;

import com.zakgof.db.ITransactional;

public interface IByteArrayKvs extends ITransactional {

  public byte[] get(byte[] keyBytes);

  public void put(byte[] keyBytes, byte[] valueBytes);

  public void delete(byte[] keyBytes);
}
