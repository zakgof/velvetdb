package com.zakgof.db.velvet.properties;

import java.util.Collection;

public interface IPropertyAccessor<K, V> {

    public Collection<String> getProperties();

    public IProperty<?, V> get(String property);

    public IProperty<K, V> getKey();

}
