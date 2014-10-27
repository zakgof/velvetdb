package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.links.IslandModel.IIslandContext;

public interface IContextSingleGetter <T> {
  public T single(IVelvet velvet, IIslandContext context); 
}
