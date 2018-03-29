package com.zakgof.db.velvet.query;

public class KeyQueries {

    public static <K> IKeyQuery<K> lt(K key) {
        return KeyQueries.<K>builder().lt(key).build();
    }

    public static <K> IKeyQuery<K> le(K key) {
        return KeyQueries.<K>builder().le(key).build();
    }

    public static <K> IKeyQuery<K> gt(K key) {
        return KeyQueries.<K>builder().gt(key).build();
    }

    public static <K> IKeyQuery<K> ge(K key) {
        return KeyQueries.<K>builder().ge(key).build();
    }

    public static <K> IKeyQuery<K> range(K low, boolean lowInclusive, K high, boolean highInclusive) {
        return KeyQueries.<K>builder().from(lowInclusive, low).to(highInclusive, high).build();
    }

    public static <K> ISingleReturnKeyQuery<K> eq(K key) {
        return KeyQueries.<K>builder().le(key).ge(key).buildSingle();
    }

    public static <K> ISingleReturnKeyQuery<K> first() {
        return KeyQueries.<K>builder().buildSingle();
    }

    public static <K> ISingleReturnKeyQuery<K> last() {
        return KeyQueries.<K>builder().descending().buildSingle();
    }

    public static <K> ISingleReturnKeyQuery<K> next(K key) {
        return KeyQueries.<K>builder().gt(key).buildSingle();
    }

    public static <K> ISingleReturnKeyQuery<K> prev(K key) {
        return KeyQueries.<K>builder().lt(key).descending().buildSingle();
    }

    public static <K> KeyQueryBuilder<K> builder() {
        return new KeyQueryBuilder<>();
    }

    public static class KeyQueryBuilder<K> {

        private boolean ascending = true;
        private int offset = 0;
        private int limit = -1;
        private IKeyAnchor<K> highAnchor;
        private IKeyAnchor<K> lowAnchor;

        private KeyQueryBuilder() {
        }

        public KeyQueryBuilder<K> ge(K key) {
            this.lowAnchor = new KeyAnchor<>(true, key);
            return this;
        }

        public KeyQueryBuilder<K> gt(K key) {
            this.lowAnchor = new KeyAnchor<>(false, key);
            return this;
        }

        public KeyQueryBuilder<K> le(K key) {
            this.highAnchor = new KeyAnchor<>(true, key);
            return this;
        }

        public KeyQueryBuilder<K> lt(K key) {
            this.highAnchor = new KeyAnchor<>(false, key);
            return this;
        }

        public KeyQueryBuilder<K> from(boolean including, K key) {
            this.lowAnchor = new KeyAnchor<>(including, key);
            return this;
        }

        public KeyQueryBuilder<K> to(boolean including, K key) {
            this.highAnchor = new KeyAnchor<>(including, key);
            return this;
        }

        public KeyQueryBuilder<K> limit(int limit) {
            this.limit = limit;
            return this;
        }

        public KeyQueryBuilder<K> offset(int offset) {
            this.offset = offset;
            return this;
        }

        public KeyQueryBuilder<K> descending() {
            this.ascending = false;
            return this;
        }

        public IKeyQuery<K> build() {
            return new KeyQuery<>(lowAnchor, highAnchor, limit, offset, ascending);
        }

        public ISingleReturnKeyQuery<K> buildSingle() {
            return limit(1).build();
        }
    }

    static class KeyAnchor<K> implements IKeyAnchor<K> {

        protected final boolean including;
        protected final K key;

        public KeyAnchor(boolean including, K key) {
            this.including = including;
            this.key = key;
        }

        @Override
        public boolean isIncluding() {
            return including;
        }

        @Override
        public K getKey() {
            return key;
        }

    }

    private static class KeyQuery<K> extends BaseQuery<IKeyAnchor<K>> implements IKeyQuery<K> {

        public KeyQuery(IKeyAnchor<K> lowAnchor, IKeyAnchor<K> highAnchor, int limit, int offset, boolean ascending) {
            super(lowAnchor, highAnchor, limit, offset, ascending);
        }

    }


}
