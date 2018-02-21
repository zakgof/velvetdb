package com.zakgof.db.velvet.query;

public interface IKeyAnchor<K> {

    boolean isIncluding();

    K getKey();
}