package com.zakgof.db.velvet.links;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public interface IMultiGetter<A, B> {
  
  public List<B> links(IVelvet velvet, A node);

  public List<Object> linkKeys(IVelvet velvet, Object key);
}
