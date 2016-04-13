package com.zakgof.db.velvet.island;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.island.IslandModel.IIslandContext;

public interface IContextSingleGetter <T> {
  public T single(IVelvet velvet, IIslandContext context);
  public String kind(); 
}
