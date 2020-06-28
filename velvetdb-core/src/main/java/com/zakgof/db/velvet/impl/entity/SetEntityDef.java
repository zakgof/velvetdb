package com.zakgof.db.velvet.impl.entity;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISetEntityDef;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import java.util.List;
import java.util.function.Function;

public class SetEntityDef<V> extends EntityDef<V, V> implements ISetEntityDef<V> {

    public SetEntityDef(Class<V> clazz, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(clazz, clazz, kind, Function.identity(), indexes);
    }

    @Override
    public IPropertyAccessor<V, V> propertyAccessor() {
        return new SelfPropertyAccessor<>(getValueClass());
    }

}
