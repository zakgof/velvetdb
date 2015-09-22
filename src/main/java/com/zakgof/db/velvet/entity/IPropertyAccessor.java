package com.zakgof.db.velvet.entity;

import java.util.Collection;

public interface IPropertyAccessor {
  
  public Collection<String> getProperties();
  
  public Object get(String property);
  
  public void put(String property, Object value);

}
