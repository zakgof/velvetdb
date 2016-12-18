package com.zakgof.db.velvet.properties;

public abstract class AReadOnlyProperty<P, V> implements IProperty<P, V> {

    public boolean isSettable() {
        return false;
    }

    public V put(V instance, P propValue) {
        throw new UnsupportedOperationException();
    }
}