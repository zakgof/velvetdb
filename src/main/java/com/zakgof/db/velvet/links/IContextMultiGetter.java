package com.zakgof.db.velvet.links;

import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.links.IslandModel.IIslandContext;

public interface IContextMultiGetter <T> {
  public Stream<T> get(IVelvet velvet, IIslandContext context); 
}
