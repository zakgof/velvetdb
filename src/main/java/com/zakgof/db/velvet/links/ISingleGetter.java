package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;

public interface ISingleGetter<A, B> {
  
  public B single(IVelvet velvet, A node);

  public Object singleKey(IVelvet velvet, Object key);
  
}
