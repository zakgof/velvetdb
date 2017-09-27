package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.impl.entity.AnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.AnnoKeyProvider;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.impl.entity.KeylessEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedAnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedEntityDef;

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

        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric) {
            indexes.add(Indexes.<M, V>create(name, metric));
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

    }
}
