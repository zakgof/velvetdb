package com.zakgof.db.velvet.impl.entity;

import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

public class SelfPropertyAccessor<V> implements IPropertyAccessor<V, V> {

    private static final String PROPERTY_NAME = "[self]";
    private IProperty<V, V> selfprop;

    SelfPropertyAccessor(Class<V> clazz) {
        this.selfprop = new IProperty<V, V>() {

            @Override
            public V get(V instance) {
                return instance;
            }

            @Override
            public Class<V> getType() {
                return clazz;
            }

            @Override
            public String getName() {
                return PROPERTY_NAME;
            }
        };
    }

    @Override
    public Collection<String> getProperties() {
        return Arrays.asList(PROPERTY_NAME);
    }

    @Override
    public IProperty<?, V> get(String property) {
        if (property.equals(PROPERTY_NAME))
            return selfprop;
        throw new NoSuchElementException();
    }

    @Override
    public IProperty<V, V> getKey() {
        return selfprop;
    }

}
