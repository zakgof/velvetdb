package com.zakgof.velvet.properties;

public interface IProperty<P, V> {

    public default boolean isSettable() {
        return false;
    }

    public P get(V instance);

    public default V put(V instance, P propValue) {
        throw new UnsupportedOperationException();
    }

    public Class<P> getType();

    public String getName();

}
