package com.zakgof.db.velvet.impl.entity;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISetEntityDef;

public class SetEntityDef<V> extends EntityDef<V, V> implements ISetEntityDef<V> {

    public SetEntityDef(Class<V> clazz, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(clazz, clazz, kind, Function.identity(), indexes);
    }

}
