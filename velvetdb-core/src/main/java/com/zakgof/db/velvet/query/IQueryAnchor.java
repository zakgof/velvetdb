package com.zakgof.db.velvet.query;

public interface IQueryAnchor<K, M extends Comparable<? super M>> {

    boolean isIncluding();

    K getKey();

    M getMetric();
}