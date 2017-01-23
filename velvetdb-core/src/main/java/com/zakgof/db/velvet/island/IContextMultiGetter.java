package com.zakgof.db.velvet.island;

import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.island.IslandModel.IIslandContext;

interface IContextMultiGetter<T> {
    public Stream<T> multi(IVelvet velvet, IIslandContext context);

    public String kind();
}
