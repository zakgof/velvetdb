package com.zakgof.db.velvet.query;

class BaseQuery<A> {
    private final boolean ascending;
    private final int offset;
    private final int limit;
    private final A highAnchor;
    private final A lowAnchor;

    public BaseQuery(A lowAnchor, A highAnchor, int limit, int offset, boolean ascending) {
        this.lowAnchor = lowAnchor;
        this.highAnchor = highAnchor;
        this.limit = limit;
        this.offset = offset;
        this.ascending = ascending;
    }

    public A getLowAnchor() {
        return lowAnchor;
    }

    public A getHighAnchor() {
        return highAnchor;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public boolean isAscending() {
        return ascending;
    }

}
