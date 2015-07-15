package com.zakgof.db.velvet.old;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public interface IMultiGetter<A, B> {
  
  public List<B> links(IVelvet velvet, A node);
  
  // Temporary
  public <K> List<K> linkKeys(IVelvet velvet, Object hostkey);

}
