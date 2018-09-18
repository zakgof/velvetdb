package com.zakgof.db.velvet.join;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.join.JoinDef.IIslandContext;

public interface IContextSingleGetter<K, V, T> {
    public T single(IVelvet velvet, IIslandContext<K, V> context);
}
