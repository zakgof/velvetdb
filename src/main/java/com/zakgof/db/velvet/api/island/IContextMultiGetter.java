package com.zakgof.db.velvet.api.island;

import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.island.IslandModel.IIslandContext;

interface IContextMultiGetter <T> {
  public Stream<T> multi(IVelvet velvet, IIslandContext context); 
}
