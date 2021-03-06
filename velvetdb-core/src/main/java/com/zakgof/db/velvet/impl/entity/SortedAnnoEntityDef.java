package com.zakgof.db.velvet.impl.entity;

import java.util.Collection;
import java.util.List;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public class SortedAnnoEntityDef<K extends Comparable<? super K>, V> extends SortedEntityDef<K, V> implements ISortableEntityDef<K, V>, IPropertyAccessor<K, V> {

    private AnnoKeyProvider<K, V> annoKeyProvider;

    public SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider, List<IStoreIndexDef<?, V>> indexes) {
        this(valueClass, annoKeyProvider, AnnoEntityDef.kindOf(valueClass), indexes);
    }

    public SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider, String kind, List<IStoreIndexDef<?, V>> indexes) {
        super(annoKeyProvider.getKeyClass(), valueClass, kind, annoKeyProvider, indexes);
        this.annoKeyProvider = annoKeyProvider;
    }

    @Override
    public Collection<String> getProperties() {
        return annoKeyProvider.getProperties();
    }

    @Override
    public IProperty<?, V> get(String property) {
        return annoKeyProvider.get(property);
    }

    @Override
    public IProperty<K, V> getKey() {
        return annoKeyProvider.getKey();
    }
}
