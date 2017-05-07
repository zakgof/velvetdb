package com.zakgof.db.velvet.query;

import com.zakgof.db.velvet.link.ISortedMultiLink;

public class Queries {

    public static <K, M extends Comparable<? super M>> Builder<K, M> builder(ISortedMultiLink<?, ?, K, ?, M> link) {
        return new Builder<>(); // TODO: pass link ?
    }

    public static <K, M extends Comparable<? super M>> Builder<K, M> builder() {
        return new Builder<>();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> range(M p1, boolean inclusive1, M p2, boolean inclusive2) {
        return Queries.<K, M> builder().from(new QueryAnchor<K, M>(inclusive1, p1)).to(new QueryAnchor<K, M>(inclusive2, p2)).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> greater(M p1) {
        return Queries.<K, M> builder().greater(p1).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> less(M p2) {
        return Queries.<K, M> builder().less(p2).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> greaterOrEq(M p1) {
        return Queries.<K, M> builder().greaterOrEq(p1).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> lessOrEq(M p2) {
        return Queries.<K, M> builder().lessOrEq(p2).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> equalsTo(M p) {
        return Queries.<K, M> builder().greaterOrEq(p).lessOrEq(p).build();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> equalsToSingle(M p) {
        return Queries.<K, M> builder().greaterOrEq(p).lessOrEq(p).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> range(int offset, int limit) {
        return Queries.<K, M> builder().offset(offset).limit(limit).build();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> first(int limit) {
        return Queries.<K, M> builder().limit(limit).build();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> first() {
        return Queries.<K, M> builder().limit(1).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> IRangeQuery<K, M> last(int limit) {
        return Queries.<K, M> builder().descending().limit(limit).build();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> last() {
        return Queries.<K, M> builder().descending().limit(1).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> prev(M m) {
        return Queries.<K, M> builder().less(m).descending().limit(1).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> next(M m) {
        return Queries.<K, M> builder().greater(m).limit(1).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> prevKey(K key) {
        return Queries.<K, M> builder().lessKey(key).descending().limit(1).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnRangeQuery<K, M> nextKey(K key) {
        return Queries.<K, M> builder().greaterKey(key).limit(1).buildSingle();
    }

    public static class Builder<K, M extends Comparable<? super M>> {

        private boolean ascending = true;
        private int offset = 0;
        private int limit = -1;
        private IQueryAnchor<K, M> highAnchor;
        private IQueryAnchor<K, M> lowAnchor;

        public Builder<K, M> greater(M value) {
            this.lowAnchor = new QueryAnchor<>(false, value);
            return this;
        }

        public Builder<K, M> greaterOrEq(M key) {
            this.lowAnchor = new QueryAnchor<>(true, key);
            return this;
        }

        public Builder<K, M> less(M key) {
            this.highAnchor = new QueryAnchor<>(false, key);
            return this;
        }

        public Builder<K, M> lessOrEq(M key) {
            this.highAnchor = new QueryAnchor<>(true, key);
            return this;
        }

        public Builder<K, M> from(IQueryAnchor<K, M> lowAnchor) {
            this.lowAnchor = lowAnchor;
            return this;
        }

        public Builder<K, M> to(IQueryAnchor<K, M> highAnchor) {
            this.highAnchor = highAnchor;
            return this;
        }

        public Builder<K, M> offset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder<K, M> limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder<K, M> descending() {
            this.ascending = false;
            return this;
        }

        public Builder<K, M> lessKey(K key) {
            this.highAnchor = QueryAnchor.byKey(key, false);
            return this;
        }

        public Builder<K, M> lessOrEqKey(K key) {
            this.highAnchor = QueryAnchor.byKey(key, true);
            return this;
        }

        public Builder<K, M> greaterKey(K key) {
            this.lowAnchor = QueryAnchor.byKey(key, false);
            return this;
        }

        public Builder<K, M> greaterOrEqKey(K key) {
            this.lowAnchor = QueryAnchor.byKey(key, true);
            return this;
        }

        public IRangeQuery<K, M> build() {
            return new Query<>(lowAnchor, highAnchor, limit, offset, ascending);
        }

        public ISingleReturnRangeQuery<K, M> buildSingle() {
            return new SingleReturnQuery<>(lowAnchor, highAnchor, limit, offset, ascending);
        }
    }

    private static class Query<K, M extends Comparable<? super M>> implements IRangeQuery<K, M> {

        private final boolean ascending;
        private final int offset;
        private final int limit;
        private final IQueryAnchor<K, M> highAnchor;
        private final IQueryAnchor<K, M> lowAnchor;

        public Query(IQueryAnchor<K, M> lowAnchor, IQueryAnchor<K, M> highAnchor, int limit, int offset, boolean ascending) {
            this.lowAnchor = lowAnchor;
            this.highAnchor = highAnchor;
            this.limit = limit;
            this.offset = offset;
            this.ascending = ascending;
        }

        @Override
        public IQueryAnchor<K, M> getLowAnchor() {
            return lowAnchor;
        }

        @Override
        public IQueryAnchor<K, M> getHighAnchor() {
            return highAnchor;
        }

        @Override
        public int getLimit() {
            return limit;
        }

        @Override
        public int getOffset() {
            return offset;
        }

        @Override
        public boolean isAscending() {
            return ascending;
        }

    }

    private static class SingleReturnQuery<K, M extends Comparable<? super M>> extends Query<K, M> implements ISingleReturnRangeQuery<K, M> {
        public SingleReturnQuery(IQueryAnchor<K, M> lowAnchor, IQueryAnchor<K, M> highAnchor, int limit, int offset, boolean ascending) {
            super(lowAnchor, highAnchor, limit, offset, ascending);
        }
    }

    private static class QueryAnchor<K, M extends Comparable<? super M>> implements IQueryAnchor<K, M> {

        private final boolean including;

        private final M value;

        private final K key;

        private static <K, M extends Comparable<? super M>> QueryAnchor<K, M> byKey(K key, boolean including) {
            return new QueryAnchor<>(including, null, key);
        }

        private QueryAnchor(boolean including, M value, K key) {
            this.including = including;
            this.key = key;
            this.value = value;
        }

        private QueryAnchor(boolean including, M value) {
            this.including = including;
            this.value = value;
            this.key = null;
        }

        @Override
        public boolean isIncluding() {
            return including;
        }

        @Override
        public M getMetric() {
            return value;
        }

        @Override
        public K getKey() {
            return key;
        }

    }

}
