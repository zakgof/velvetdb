package com.zakgof.db;

public interface ILockable {

  public void lock(String lockName, long timeout);

  public void unlock(String lockName);

}
