package com.zakgof.db.velvet.links.index;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public interface IIndexedGetter<A, B, C extends Comparable<C>> {
  
  public List<B> links(IVelvet velvet, A node, IndexQuery<B, C> indexQuery);

  public List<Object> linkKeys(IVelvet velvet, Object key, IndexQuery<B, C> indexQuery);
  
}
