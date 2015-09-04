package com.zakgof.db.velvet.test;

import java.io.Serializable;

import com.zakgof.db.velvet.annotation.Key;

public class TestEnt implements Serializable {

  private static final long serialVersionUID = -624857516746316365L;
  
  private float val;
  private String key;

  public TestEnt() {
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + Float.floatToIntBits(val);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TestEnt other = (TestEnt) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (Math.abs(val - other.val) > 1e-6)
      return false;
    return true;
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