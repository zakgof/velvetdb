package com.zakgof.velvet.impl.entity;

public interface IIndexRequest<K, V, M> {


    boolean descending();

    int limit();
    int offset();

    IBound<K, M> upper();
    IBound<K, M> lower();

    IndexDef<K, V, M> indexDef();

    interface IBound<K, M> {
        K key();
        M index();
        boolean inclusive();
    }
}
