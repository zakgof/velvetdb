package com.zakgof.db.velvet.properties;

public abstract class AProperty<P, V> implements IProperty<P, V> {

    @Override
    public boolean isSettable() {
        return false;
    }

    @Override
    public V put(V instance, P propValue) {
        throw new UnsupportedOperationException();
    }

}
