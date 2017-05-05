package com.zakgof.db.velvet.island;

import com.zakgof.db.velvet.IVelvet;

public interface IContextSingleGetter<K, V, T> {
    public T single(IVelvet velvet, IIslandContext<K, V> context);
}
