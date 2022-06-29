package com.zakgof.velvet.impl.entity;

public interface IIndexRequest<K, V, M> {


    boolean descending();

    IBound<K, V, M> upper(); // TODO

    IBound<K, V, M> lower();

    IndexDef<K, V, M> indexDef();

    interface IBound<K, V, M> {
        K key();
        M index();
        boolean inclusive();
    }
}
