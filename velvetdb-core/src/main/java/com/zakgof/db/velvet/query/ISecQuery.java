package com.zakgof.db.velvet.query;

public interface ISecQuery<K, M extends Comparable<? super M>> extends ISingleReturnSecQuery<K, M>{

    ISecAnchor<K, M> getLowAnchor();

    ISecAnchor<K, M> getHighAnchor();

    int getLimit();

    int getOffset();

    boolean isAscending();

}