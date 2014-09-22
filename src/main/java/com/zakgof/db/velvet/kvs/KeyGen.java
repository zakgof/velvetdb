package com.zakgof.db.velvet.kvs;

import java.io.Serializable;
import java.util.Arrays;

public class KeyGen {

  public static Serializable key(String str, byte[] bytes) {
    return new Key(str, bytes);
  }
  
  static class Key implements Serializable {
  
    private static final long serialVersionUID = -852344002157295111L;
    private final int hash;
    private final byte[] bytes;
    private final String str;

    public Key(String str, byte[] bytes) {
      this.hash = str.hashCode() + 29 * Arrays.hashCode(bytes);
      this.str = str;
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      return hash;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Key))
        return false;
      Key that = (Key)obj;
      if (hash != that.hash || !this.str.equals(that.str))
        return false;
      return Arrays.equals(this.bytes, that.bytes);
    }
    
    @Override
    public String toString() {
      return "Key(" + hash + ")";
    }
    
  }

  public static CompositeKey key(String prefix, Object key) {
    return new CompositeKey(prefix, key);
  }

}
