package com.zakgof.db.velvet.kvs;

import java.io.UnsupportedEncodingException;

import com.zakgof.serialize.ZeSerializer;

public class KeyGen {
  
  /*

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
  
  */

  public static Object key(String prefix, Object key) {    
    try {
      
      if (key instanceof String)
        return prefix + "/" + (String)key;
      
      ZeSerializer serializer = new ZeSerializer();
      byte[] prefixBytes = prefix.getBytes("utf-8");
      byte[] bodyBytes = serializer.serialize(key);
      return concat(prefixBytes, bodyBytes);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("string to bytes error"); // TODO
    }        
  }
  
  private static byte[] concat(byte[] a, byte[] b) {
    int aLen = a.length;
    int bLen = b.length;
    byte[] c = new byte[aLen + bLen];
    System.arraycopy(a, 0, c, 0, aLen);
    System.arraycopy(b, 0, c, aLen, bLen);
    return c;
  }

}
