package com.zakgof.db.velvet.impl.entity;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.annotation.Kind;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public class AnnoEntityDef<K, V> extends EntityDef<K, V> implements IPropertyAccessor<K, V> {

    private AnnoKeyProvider<K, V> annoKeyProvider;

    public AnnoEntityDef(Class<V> valueClass, String kind, AnnoKeyProvider<K, V> annoKeyProvider, List<IStoreIndexDef<?, V>> indexes) {
        super(annoKeyProvider.getKeyClass(), valueClass, kind, annoKeyProvider, indexes);
        this.annoKeyProvider = annoKeyProvider;
    }

    public static String kindOf(Class<?> clazz) {
        Kind annotation = clazz.getAnnotation(Kind.class);
        if (annotation != null)
            return annotation.value();
        String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
        return kind;
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
