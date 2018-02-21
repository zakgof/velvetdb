package com.zakgof.db.velvet.query;

public interface IRangeQuery<A extends IKeyAnchor<?>> {

    A getLowAnchor();

    A getHighAnchor();

    int getLimit();

    int getOffset();

    boolean isAscending();
}
