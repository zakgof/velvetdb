package com.zakgof.db.velvet;

import java.util.List;

public interface IVelvet {

  public <T> T get(Class<T> clazz, Object key);

  public void put(Object node);
  
  public void delete(Object node);

  public IRawVelvet raw();
  
  public <T> List<T> allOf(Class<T> clazz);

  public <T> List<T> links(Class<T> clazz, Object node, String edgeKind);
  
  public void connect(Object node1, Object node2, String kind);
  
  public void disconnect(Object node1, Object node2, String kind);
  

}
