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

/**
 * Utility class for creating entities.
 */
final public class Entities {

    /**
     * Creates an entity definition from a class. The class should have one {@code Key} annotation that identifies primary key field or method.
     * @param valueClass value class
     * @param <K> key class
     * @param <V> value class
     * @return entity definition
     */
    public static <K, V> IEntityDef<K, V> create(Class<V> valueClass) {
        return from(valueClass).make();
    }

    /**
     * Creates a keyless entity definition from a class. Keyless entity do not hold a key, an autogenerated long key is provided by the database.
     * @param valueClass value class
     * @param <V> value class
     * @return entity definition
     */
    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass) {
        return from(valueClass).makeKeyless();
    }

    /**
     * Creates a sortable entity definition from a class. The class should have one {@code SortedKey} annotation.
     * @param valueClass value class
     * @param <K> sortable key class
     * @param <V> value class
     * @return entity definition
     */
    public static <K extends Comparable<? super K>, V> ISortableEntityDef<K, V> sorted(Class<V> valueClass) {
        return from(valueClass).makeSorted();
    }

    /**
     * Creates a set entity definition - an entity which key is its value itself.
     * @param valueClass key and value class
     * @param <V> key and value class
     * @return entity definition
     */
    public static <V> ISetEntityDef<V> set(Class<V> valueClass) {
        return from(valueClass).makeSet();
    }

    /**
     * Creates a set entity definition - an entity which sortable key is its value itself.
     * @param valueClass key and value class
     * @param <V> key and value class
     * @return entity definition
     */
    public static <V extends Comparable<? super V>> ISortableSetEntityDef<V> sortedSet(Class<V> valueClass) {
        return from(valueClass).makeSortedSet();
    }

    /**
     * Creates a builder for advanced entity definition construction.
     * @param clazz entity class
     * @param <V> entity class
     * @return entity definition builder
     */
    public static <V> Builder<V> from(Class<V> clazz) {
        return new Builder<>(clazz);
    }

    /**
     * Builder for advanced entity construction.
     * @param <V> entity value class
     */
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

        /**
         * Adds secondary index to the entity definition being constructed.
         * @param name index name
         * @param metric function returning index value from the entity
         * @param metricClass index value class
         * @param <M> index value class
         * @return this builder
         */
        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric, Class<M> metricClass) {
            indexes.add(new Index<>(name, metric, metricClass));
            return this;
        }

        /**
         * Sets entity name.
         * @param kind entity name
         * @return this builder
         */
        public Builder<V> kind(String kind) {
            this.kind = kind;
            return this;
        }

        /**
         * Creates an entity definition. Primary key should be assigned using the {@code Key} annotation.
         * @param <K> primary key type
         * @return entity definition
         */
        @SuppressWarnings("unchecked")
        public <K> IEntityDef<K, V> make() {
            return new AnnoEntityDef<>(clazz, kind, (AnnoKeyProvider<K, V>)annoKeyProvider, indexes);
        }

        /*
        @SuppressWarnings("unchecked")
        public  <K> IEntityDef<K, V> make(Class<K> keyClass) {
            return new EntityDef<>(keyClass, clazz, kind, (AnnoKeyProvider<K, V>)annoKeyProvider, indexes);
        }
        */

        /**
         * Creates an entity definition explicitly specifying primary key.
         * @param keyClass primary key class
         * @param keyFunction function returning primary key from an entity
         * @param <K> primary key type
         * @return entity definition
         */
        public  <K> IEntityDef<K, V> make(Class<K> keyClass, Function<V, K> keyFunction) {
            return new EntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        /**
         * Creates an entity definition. Primary key must be specified using the {@code Key} annotation.
         * @return entity definition
         */
        public IKeylessEntityDef<V> makeKeyless() {
            return new KeylessEntityDef<>(clazz, kind, indexes);
        }

        /**
         * Creates a sortable entity definition. Primary key must be specified using the {@code SortedKey} annotation and must implement {@code Comparable}.
         * @param <K> primary key type
         * @return entity definition
         */
        @SuppressWarnings("unchecked")
        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted() {
            return new SortedAnnoEntityDef<>(clazz, (AnnoKeyProvider<K, V>)annoKeyProvider, kind, indexes);
        }

        /**
         * Creates a sortable entity definition explicitly specifying the primary key.
         * @param keyClass primary key class
         * @param <K> primary key class
         * @param keyFunction function returning primary key from an entity
         * @return entity definition
         */
        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted(Class<K> keyClass, Function<V, K> keyFunction) {
            return new SortedEntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        /**
         * Creates a set entity definition - entity which key is its value itself.
         * @return entity definition
         */
        public ISetEntityDef<V> makeSet() {
            return new SetEntityDef<>(clazz, kind, indexes);
        }

        /**
         * Creates a sorted set entity definition - entity which key is its value itself. The entity class must implement {@code Comparable}.
         * @return entity definition
         * @param <V> key/value class
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <VS extends Comparable<? super VS>> ISortableSetEntityDef<VS> makeSortedSet() {
            return new SortedSetEntityDef(clazz, kind, indexes);
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
