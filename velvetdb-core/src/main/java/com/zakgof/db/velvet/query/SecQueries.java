package com.zakgof.db.velvet.query;

import com.zakgof.db.velvet.query.KeyQueries.KeyAnchor;

public class SecQueries {

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> eq(M m) {
        return SecQueries.<K, M> builder().le(m).ge(m).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> lt(M m) {
        return SecQueries.<K, M> builder().lt(m).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> gt(M m) {
        return SecQueries.<K, M> builder().gt(m).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> le(M m) {
        return SecQueries.<K, M> builder().le(m).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> ge(M m) {
        return SecQueries.<K, M> builder().ge(m).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> ltKey(K key) {
        return SecQueries.<K, M> builder().ltKey(key).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> leKey(K key) {
        return SecQueries.<K, M> builder().leKey(key).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> gtKey(K key) {
        return SecQueries.<K, M> builder().gtKey(key).build();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> geKey(K key) {
        return SecQueries.<K, M> builder().geKey(key).build();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnSecQuery<K, M> eqKey(K key) {
        return SecQueries.<K, M> builder().leKey(key).geKey(key).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnSecQuery<K, M> first() {
        return SecQueries.<K, M> builder().buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnSecQuery<K, M> last() {
        return SecQueries.<K, M> builder().descending().buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnSecQuery<K, M> next(K key) {
        return SecQueries.<K, M> builder().gtKey(key).buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISingleReturnSecQuery<K, M> prev(K key) {
        return SecQueries.<K, M> builder().ltKey(key).descending().buildSingle();
    }

    public static <K, M extends Comparable<? super M>> ISecQuery<K, M> range(M from, boolean fromInclusive, M to, boolean toInclusive) {
        return SecQueries.<K, M> builder().from(fromInclusive, null, from).to(toInclusive, null, to).build();
    }

    public static <K, M extends Comparable<? super M>> SecQueryBuilder<K, M> builder() {
        return new SecQueryBuilder<>();
    }

    public static <K extends Comparable<? super K>> ISecQuery<K, K> from(IKeyQuery<K> keyQuery) {
        SecQueryBuilder<K, K> builder = new SecQueryBuilder<>();
        IKeyAnchor<K> highAnchor = keyQuery.getHighAnchor();
        if (highAnchor != null) {
            builder.to(highAnchor.isIncluding(), highAnchor.getKey(), highAnchor.getKey());
        }
        IKeyAnchor<K> lowAnchor = keyQuery.getLowAnchor();
        if (lowAnchor != null) {
            builder.from(lowAnchor.isIncluding(), lowAnchor.getKey(), lowAnchor.getKey());
        }
        builder.limit(keyQuery.getLimit()).offset(keyQuery.getOffset());
        if (!keyQuery.isAscending()) {
            builder.descending();
        }
        return builder.build();
    }

    public static class SecQueryBuilder<K, M extends Comparable<? super M>> {

        private boolean ascending = true;
        private int offset = 0;
        private int limit = -1;
        private ISecAnchor<K, M> highAnchor;
        private ISecAnchor<K, M> lowAnchor;

        private SecQueryBuilder() {
        }

        public SecQueryBuilder<K, M> from(boolean including, K key, M metric) {
            this.lowAnchor = new SecAnchor<>(including, key, metric);
            return this;
        }

        public SecQueryBuilder<K, M> to(boolean including, K key, M metric) {
            this.highAnchor = new SecAnchor<>(including, key, metric);
            return this;
        }

        public SecQueryBuilder<K, M> le(M m) {
            return to(true, null, m);
        }

        public SecQueryBuilder<K, M> lt(M m) {
            return to(false, null, m);
        }

        public SecQueryBuilder<K, M> ge(M m) {
            return from(true, null, m);
        }

        public SecQueryBuilder<K, M> gt(M m) {
            return from(false, null, m);
        }

        public SecQueryBuilder<K, M> leKey(K key) {
            return to(true, key, null);
        }

        public SecQueryBuilder<K, M> ltKey(K key) {
            return to(false, key, null);
        }

        public SecQueryBuilder<K, M> geKey(K key) {
            return from(true, key, null);
        }

        public SecQueryBuilder<K, M> gtKey(K key) {
            return from(false, key, null);
        }

        public SecQueryBuilder<K, M> limit(int limit) {
            this.limit = limit;
            return this;
        }

        public SecQueryBuilder<K, M> offset(int offset) {
            this.offset = offset;
            return this;
        }

        public SecQueryBuilder<K, M> descending() {
            this.ascending = false;
            return this;
        }

        public ISecQuery<K, M> build() {
            return new SecQuery<>(lowAnchor, highAnchor, limit, offset, ascending);
        }

        public ISingleReturnSecQuery<K, M> buildSingle() {
            return limit(1).build();
        }
    }

    private static class SecAnchor<K, M extends Comparable<? super M>> extends KeyAnchor<K> implements ISecAnchor<K, M> {

        private final M metric;

        public SecAnchor(boolean including, K key, M metric) {
            super(including, key);
            this.metric = metric;
        }

        @Override
        public M getMetric() {
            return metric;
        }

    }

    private static class SecQuery<K, M extends Comparable<? super M>> extends BaseQuery<ISecAnchor<K, M>> implements ISecQuery<K, M> {

        public SecQuery(ISecAnchor<K, M> lowAnchor, ISecAnchor<K, M> highAnchor, int limit, int offset, boolean ascending) {
            super(lowAnchor, highAnchor, limit, offset, ascending);
        }

    }

}
