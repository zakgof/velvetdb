package com.zakgof.db.velvet.old;

import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.old.IslandModel.IIslandContext;

public interface IContextMultiGetter <T> {
  public Stream<T> get(IVelvet velvet, IIslandContext context); 
}
