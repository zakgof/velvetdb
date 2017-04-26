package com.zakgof.db.velvet.island;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.island.IslandModel.IIslandContext;

public interface IContextSingleGetter<K, V, T> {
    public T single(IVelvet velvet, IIslandContext<K, V> context);
}
