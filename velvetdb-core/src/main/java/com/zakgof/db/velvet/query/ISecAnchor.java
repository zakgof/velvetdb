package com.zakgof.db.velvet.query;

public interface ISecAnchor<K, M extends Comparable<? super M>> extends IKeyAnchor<K> {
    M getMetric();
}