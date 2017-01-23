package com.zakgof.db.velvet.query;

public interface IRangeQuery<K, M extends Comparable<? super M>> {

    IQueryAnchor<K, M> getLowAnchor();

    IQueryAnchor<K, M> getHighAnchor();

    int getLimit();

    int getOffset();

    boolean isAscending();

}