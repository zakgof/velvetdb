package com.zakgof.velvet.request;

import com.zakgof.velvet.impl.index.IndexDef;

public interface IIndexQuery<K, V, I> {
    boolean descending();

    int limit();
    int offset();

    IBound<K, I> upper();
    IBound<K, I> lower();

    IndexDef<K, V, I> indexDef();

    interface IBound<K, M> {
        K key();
        M index();
        boolean inclusive();
    }
}
