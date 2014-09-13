package com.zakgof.db;

public interface ITransactional {
  public void rollback();

  public void commit();
}
