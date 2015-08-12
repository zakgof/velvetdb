package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.annotation.Key;

public class TestEnt2 {
  
  private int key;
  private String val;

  public TestEnt2() {
  }

  public TestEnt2(int key) {
    this.key = key;
    this.val = "v" + key;
  }

  public String getVal() {
    return val;
  }

  @Key
  public int getKey() {
    return key;
  }

  @Override
  public String toString() {
    return key + " " + val;
  }
}