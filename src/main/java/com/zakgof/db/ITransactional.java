package com.zakgof.db;

public interface ITransactional {
  
  public void begin();
  
  public void rollback();

  public void commit();
}
