package com.zakgof.db.velvet.old;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.old.IslandModel.IIslandContext;

public interface IContextSingleGetter <T> {
  public T single(IVelvet velvet, IIslandContext context); 
}
