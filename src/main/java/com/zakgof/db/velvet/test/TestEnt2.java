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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + key;
    result = prime * result + ((val == null) ? 0 : val.hashCode());
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
    TestEnt2 other = (TestEnt2) obj;
    if (key != other.key)
      return false;
    if (val == null) {
      if (other.val != null)
        return false;
    } else if (!val.equals(other.val))
      return false;
    return true;
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