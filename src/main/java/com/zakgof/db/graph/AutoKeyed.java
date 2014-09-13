package com.zakgof.db.graph;

import com.zakgof.db.graph.annotation.AutoKey;

public abstract class AutoKeyed {
  
  @AutoKey
  protected Long key;
  
  @Override
  public boolean equals(Object o) {
    return key.equals(((AutoKeyed)o).key);
  }
  
  @Override
  public int hashCode() {
    return (int) (key + 31 * getClass().hashCode());
  }
  
  public Long getKey() {
    return key;
  }
}
