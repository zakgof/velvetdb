package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.annotation.Key;

public class TestEnt {

  private float val;
  private String key;

  public TestEnt() {
  }

  public TestEnt(String key, float val) {
    this.key = key;
    this.val = val;
  }

  public float getVal() {
    return val;
  }

  @Key
  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return key + " " + val;
  }
}