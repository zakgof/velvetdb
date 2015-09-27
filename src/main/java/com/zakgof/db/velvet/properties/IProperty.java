package com.zakgof.db.velvet.properties;

public interface IProperty<P, V> {
  
  public boolean isSettable();
  
  public P get(V instance);
  
  public void put(V instance, P propValue);
  
  public Class<P> getType();

  public String getName();

}
