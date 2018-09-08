package com.zakgof.db.velvet.impl.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISetEntityDef;

public class SetEntityDef<V> extends EntityDef<V, V> implements ISetEntityDef<V> {

    public SetEntityDef(Class<V> clazz, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(clazz, clazz, kind, x -> x, indexes);
    }

}
