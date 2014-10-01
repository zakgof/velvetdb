package com.zakgof.db.velvet.kvs;

public class CompositeKey {

  @Deprecated
  public CompositeKey() {
  }

  private String prefix;
  
  private Object key;

  public CompositeKey(String prefix, Object key) {
    this.prefix = prefix;
    this.key = key;
  }

  @Override
  public String toString() {
    return "(" + prefix + " - " + key + ")";
  }

  @Override
  public boolean equals(Object o) {
    CompositeKey that = (CompositeKey) o;
    return key.equals(that.key) && prefix.equals(that.prefix);
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + prefix.hashCode();
  }

}