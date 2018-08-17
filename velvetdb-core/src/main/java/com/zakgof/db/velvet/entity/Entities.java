package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.impl.entity.AnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.AnnoKeyProvider;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.impl.entity.KeylessEntityDef;
import com.zakgof.db.velvet.impl.entity.SetEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedAnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedSetEntityDef;

final public class Entities {

    public static <K, V> IEntityDef<K, V> create(Class<V> valueClass) {
        return from(valueClass).make();
    }

    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass) {
        return from(valueClass).makeKeyless();
    }

    public static <K extends Comparable<? super K>, V> ISortableEntityDef<K, V> sorted(Class<V> valueClass) {
        return from(valueClass).makeSorted();
    }

    public static <V> ISetEntityDef<V> set(Class<V> valueClass) {
        return from(valueClass).makeSet();
    }

    public static <V> Builder<V> from(Class<V> clazz) {
        return new Builder<>(clazz);
    }

    public static class Builder<V> {

        private Class<V> clazz;
        private List<IStoreIndexDef<?, V>> indexes = new ArrayList<>();
        private String kind;
        private AnnoKeyProvider<?, V> annoKeyProvider;

        private Builder (Class<V> clazz) {
            this.clazz = clazz;
            this.kind = AnnoEntityDef.kindOf(clazz);
            this.annoKeyProvider = new AnnoKeyProvider<>(clazz);
            this.indexes = annoKeyProvider.getIndexes();
        }

        public ISetEntityDef<V> makeSet() {
            return new SetEntityDef<>(clazz, kind, indexes);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public ISetEntityDef<V> makeSortedSet() {
            return new SortedSetEntityDef(clazz, kind, indexes);
        }

        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric, Class<M> metricClass) {
            indexes.add(new Index<>(name, metric, metricClass));
            return this;
        }

        public Builder<V> kind(String kind) {
            this.kind = kind;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <K> IEntityDef<K, V> make() {
            return new AnnoEntityDef<>(clazz, kind, (AnnoKeyProvider<K, V>)annoKeyProvider, indexes);
        }

        @SuppressWarnings("unchecked")
        public  <K> IEntityDef<K, V> make(Class<K> keyClass) {
            return new EntityDef<>(keyClass, clazz, kind, (AnnoKeyProvider<K, V>)annoKeyProvider, indexes);
        }

        public  <K> IEntityDef<K, V> make(Class<K> keyClass, Function<V, K> keyFunction) {
            return new EntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        public IKeylessEntityDef<V> makeKeyless() {
            return new KeylessEntityDef<>(clazz, kind, indexes);
        }

        @SuppressWarnings("unchecked")
        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted() {
            return new SortedAnnoEntityDef<>(clazz, (AnnoKeyProvider<K, V>)annoKeyProvider, kind, indexes);
        }

        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted(Class<K> keyClass, Function<V, K> keyFunction) {
            return new SortedEntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        private class Index<M extends Comparable<? super M>> implements IStoreIndexDef<M , V> {

            private final String name;
            private final Function<V, M> metric;
            private final Class<M> metricClass;

            private Index(String name, Function<V, M> metric, Class<M> metricClass) {
                this.name = name;
                this.metric = metric;
                this.metricClass = metricClass;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public Function<V, M> metric() {
                return metric;
            }

            @Override
            public Class<M> clazz() {
                return metricClass;
            }

        }

    }
}
