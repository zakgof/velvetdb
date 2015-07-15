package com.zakgof.db.velvet.api.island;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.island.IslandModel.IIslandContext;

interface IContextSingleGetter <T> {
  public T single(IVelvet velvet, IIslandContext context); 
}
