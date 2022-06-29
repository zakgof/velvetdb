package com.zakgof.velvet.entity;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.velvet.annotation.Index;
import com.zakgof.velvet.annotation.Key;
import com.zakgof.velvet.annotation.Kind;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.IndexInfo;
import com.zakgof.velvet.request.IEntityDef;
import lombok.SneakyThrows;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for creating entities.
 */
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

    public static <V extends Comparable<? super V>> ISortableSetEntityDef<V> sortedSet(Class<V> valueClass) {
        return from(valueClass).makeSortedSet();
    }

    public static <V> Builder<V> from(Class<V> clazz) {
        return new Builder<>(clazz);
    }

    /**
     * Builder for advanced entity construction.
     *
     * @param <V> entity value class
     */
    public static class Builder<V> {

        private final Class<V> valueClass;
        private List<IndexInfo<?, V>> indexes = new ArrayList<>();
        private IndexInfo<?, V> key;
        private String kind;

        private Builder(Class<V> clazz) {
            this.valueClass = clazz;
            this.kind = kindOf(clazz);

            List<Field> fields = getAllFields(clazz);
            List<Method> methods = Arrays.asList(clazz.getMethods()); // TODO ?

            this.key = key(fields, methods);
            this.indexes = (List)indexes(fields, methods);
        }

        static List<Field> getAllFields(Class<?> type) {
            List<Field> fields = new ArrayList<>();
            Field[] declaredFields = type.getDeclaredFields();
            Arrays.sort(declaredFields, Comparator.comparing(Field::getName));
            for (Field field : declaredFields) {
                if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                    fields.add(field);
                }
            }
            if (type.getSuperclass() != null)
                fields.addAll(getAllFields(type.getSuperclass()));
            return fields;
        }

        private IndexInfo<?, V> key(List<Field> fields, List<Method> methods) {
            List<IndexInfo> keyInfos = fields.stream()
                    .flatMap(field -> anno(field, Key.class)
                            .map(anno -> createKey(field, anno))
                    ).collect(Collectors.toList());

            // TODO: scan methods

            if (keyInfos.size() > 1) {
                throw new VelvetException("Multiple @Key annotations found");
            } else if (keyInfos.size() == 1) {
                return keyInfos.get(0);
            } else {
                return null;
            }
        }

        private IndexInfo createKey(Field field, Key anno) {
            field.setAccessible(true);
            return new IndexInfo(null, field.getType(), v -> fieldGet(field, v));
        }

        private List<IndexInfo> indexes(List<Field> fields, List<Method> methods) {
            return fields.stream()
                    .flatMap(field -> anno(field, Index.class)
                            .map(anno -> createIndex(field, anno))
                    ).collect(Collectors.toList());

            // TODO scan methods
        }

        private IndexInfo createIndex(Field field, Index anno) {
            String name = anno.name().isEmpty() ? field.getName() : anno.name();
            return new IndexInfo(name, field.getType(), v -> fieldGet(field, v));
        }

        @SneakyThrows
        private Object fieldGet(Field field, Object value) {
            return field.get(value);
        }

        private <A extends Annotation> Stream<A> anno(Field field, Class<A> annoClass) {
            A anno = field.getAnnotation(annoClass);
            return anno == null ? Stream.empty() : Stream.of(anno);
        }

        private static String kindOf(Class<?> clazz) {
            Kind annotation = clazz.getAnnotation(Kind.class);
            if (annotation != null)
                return annotation.value();
            String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
            return kind;
        }

        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric, Class<M> metricClass) {
            indexes.add(new IndexInfo<M, V>(name, metricClass, metric));
            return this;
        }

        /**
         * Sets entity name.
         *
         * @param kind entity name
         * @return this builder
         */
        public Builder<V> kind(String kind) {
            this.kind = kind;
            return this;
        }

        /**
         * Creates an entity definition. Primary key should be assigned using the {@code Key} annotation.
         *
         * @param <K> primary key type
         * @return entity definition
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <K> IEntityDef<K, V> make() {
            if (key == null) {
                throw new VelvetException("No key defined");
            }
            return new EntityDef<K, V>(kind, valueClass, (IndexInfo) key, indexes);
        }

        /**
         * Creates an entity definition explicitly specifying primary key.
         *
         * @param keyClass    primary key class
         * @param keyFunction function returning primary key from an entity
         * @param <K>         primary key type
         * @return entity definition
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <K> IEntityDef<K, V> make(Class<K> keyClass, Function<V, K> keyFunction) {
            IndexInfo<K, V> keyIndex = new IndexInfo<>(null, keyClass, keyFunction);
            return new EntityDef<K, V>(kind, valueClass, keyIndex, indexes);
        }

        /**
         * Creates an entity definition. Primary key must be specified using the {@code Key} annotation.
         *
         * @return entity definition
         */
        public IKeylessEntityDef<V> makeKeyless() {
            // TODO
            return null; // new KeylessEntityDef<>(clazz, kind, indexes);
        }

        /**
         * Creates a sortable entity definition. Primary key must be specified using the {@code SortedKey} annotation and must implement {@code Comparable}.
         *
         * @param <K> primary key type
         * @return entity definition
         */
        @SuppressWarnings("unchecked")
        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted() {
            // TODO
            return null; // new SortedAnnoEntityDef<>(clazz, (AnnoKeyProvider<K, V>) annoKeyProvider, kind, indexes);
        }

        /**
         * Creates a sortable entity definition explicitly specifying the primary key.
         *
         * @param keyClass    primary key class
         * @param <K>         primary key class
         * @param keyFunction function returning primary key from an entity
         * @return entity definition
         */
        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted(Class<K> keyClass, Function<V, K> keyFunction) {
            // TODO
            return null; //new SortedEntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        /**
         * Creates a set entity definition - entity which key is its value itself.
         *
         * @return entity definition
         */
        public ISetEntityDef<V> makeSet() {
            // TODO
            return null; //new SetEntityDef<>(clazz, kind, indexes);
        }

        /**
         * Creates a sorted set entity definition - entity which key is its value itself. The entity class must implement {@code Comparable}.
         *
         * @param <VS> key and value class
         * @return entity definition
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <VS extends Comparable<? super VS>> ISortableSetEntityDef<VS> makeSortedSet() {
            // TODO
            return null; //new SortedSetEntityDef(clazz, kind, indexes);
        }

    }
}
