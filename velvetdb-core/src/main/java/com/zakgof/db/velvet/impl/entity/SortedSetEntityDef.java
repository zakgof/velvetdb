package com.zakgof.db.velvet.impl.entity;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISortableSetEntityDef;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import java.util.List;
import java.util.function.Function;

public class SortedSetEntityDef<V extends Comparable<? super V>> extends SortedEntityDef<V, V> implements ISortableSetEntityDef<V> {

    public SortedSetEntityDef(Class<V> clazz, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(clazz, clazz, kind, Function.identity(), indexes);
    }

    @Override
    public IPropertyAccessor<V, V> propertyAccessor() {
        return new SelfPropertyAccessor<>(getValueClass());
    }

}
