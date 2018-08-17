package com.zakgof.db.velvet.impl.entity;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISortableSetEntityDef;

public class SortedSetEntityDef<V extends Comparable<? super V>> extends SortedEntityDef<V, V> implements ISortableSetEntityDef<V> {

    public SortedSetEntityDef(Class<V> clazz, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(clazz, clazz, kind, Function.identity(), indexes);
    }

}
