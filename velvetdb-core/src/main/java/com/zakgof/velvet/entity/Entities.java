package com.zakgof.velvet.entity;

import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.annotation.Index;
import com.zakgof.velvet.annotation.Key;
import com.zakgof.velvet.annotation.Kind;
import com.zakgof.velvet.annotation.SortedKey;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.SortedEntityDef;
import com.zakgof.velvet.impl.index.IndexInfo;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for creating entities.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Entities {

    public static <K, V> IEntityDef<K, V> create(Class<V> valueClass) {
        return from(valueClass).make();
    }

    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass) {
        return from(valueClass).makeKeyless();
    }

    public static <K extends Comparable<? super K>, V> ISortedEntityDef<K, V> sorted(Class<V> valueClass) {
        return from(valueClass).makeSorted();
    }

    public static <V> ISetEntityDef<V> set(Class<V> valueClass) {
        return from(valueClass).makeSet();
    }

    public static <V extends Comparable<? super V>> ISortedSetEntityDef<V> sortedSet(Class<V> valueClass) {
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
            List<Method> methods = Arrays.asList(clazz.getDeclaredMethods());

            this.key = key(fields, methods);
            this.indexes = (List) indexes(fields, methods);
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
            List<IndexInfo> fieldKeyInfos = fields.stream()
                    .filter(field -> hasAnno(field, Key.class))
                    .map(this::createKey)
                    .collect(Collectors.toList());
            List<IndexInfo> fieldSortedKeyInfos = fields.stream()
                    .filter(field -> hasAnno(field, SortedKey.class))
                    .map(this::createKey)
                    .collect(Collectors.toList());
            List<IndexInfo> methodKeyInfos = methods.stream()
                    .filter(method -> hasAnno(method, Key.class))
                    .peek(this::ensureNoArgs)
                    .map(this::createKey)
                    .collect(Collectors.toList());
            List<IndexInfo> methodSortedKeyInfos = methods.stream()
                    .filter(method -> hasAnno(method, SortedKey.class))
                    .peek(this::ensureNoArgs)
                    .map(this::createKey)
                    .collect(Collectors.toList());

            // TODO: scan methods
            List<IndexInfo> allKeys = new ArrayList<>();
            allKeys.addAll(fieldKeyInfos);
            allKeys.addAll(fieldSortedKeyInfos);
            allKeys.addAll(methodKeyInfos);
            allKeys.addAll(methodSortedKeyInfos);

            if (allKeys.size() > 1) {
                throw new VelvetException("Multiple Key annotations found");
            } else if (allKeys.size() == 1) {
                return allKeys.get(0);
            } else {
                return null;
            }
        }

        private void ensureNoArgs(Method method) {
            if (method.getParameterCount() > 0) {
                throw new VelvetException("Only no-arg methods can have @Key, @SortedKey or @Index annotations");
            }
        }

        private boolean hasAnno(AnnotatedElement annotatedElement, Class<? extends Annotation> annoClass) {
            Object anno = annotatedElement.getAnnotation(annoClass);
            return anno != null;
        }

        private IndexInfo createKey(Field field) {
            field.setAccessible(true);
            return new IndexInfo(null, field.getType(), v -> fieldGet(field, v));
        }

        private IndexInfo createKey(Method method) {
            method.setAccessible(true);
            return new IndexInfo(null, method.getReturnType(), v -> methodGet(method, v));
        }

        private List<IndexInfo> indexes(List<Field> fields, List<Method> methods) {
            return Stream.concat(
                    fields.stream()
                            .flatMap(field -> anno(field, Index.class)
                                    .map(anno -> createIndex(field, anno))
                            ),
                    methods.stream()
                            .flatMap(method -> anno(method, Index.class)
                                    .peek(anno -> ensureNoArgs(method))
                                    .map(anno -> createIndex(method, anno))
                            )
            ).collect(Collectors.toList());
        }

        private IndexInfo createIndex(Field field, Index anno) {
            field.setAccessible(true);
            String name = anno.name().isEmpty() ? field.getName() : anno.name();
            return new IndexInfo(name, field.getType(), v -> fieldGet(field, v));
        }

        private IndexInfo createIndex(Method method, Index anno) {
            method.setAccessible(true);
            String name = anno.name().isEmpty() ? method.getName() : anno.name();
            return new IndexInfo(name, method.getReturnType(), v -> methodGet(method, v));
        }

        @SneakyThrows
        private Object fieldGet(Field field, Object value) {
            return field.get(value);
        }

        @SneakyThrows
        private Object methodGet(Method method, Object value) {
            return method.invoke(value);
        }

        private <A extends Annotation> Stream<A> anno(AnnotatedElement element, Class<A> annoClass) {
            A anno = element.getAnnotation(annoClass);
            return anno == null ? Stream.empty() : Stream.of(anno);
        }

        private static String kindOf(Class<?> clazz) {
            Kind annotation = clazz.getAnnotation(Kind.class);
            if (annotation != null)
                return annotation.value();
            return clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
        }

        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric, Class<M> metricClass) {
            indexes.add(new IndexInfo<>(name, metricClass, metric));
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
            return new EntityDef<>(kind, valueClass, (IndexInfo) key, indexes);
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
            return new EntityDef<>(kind, valueClass, keyIndex, indexes);
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
        public <K extends Comparable<? super K>> ISortedEntityDef<K, V> makeSorted() {
            if (key == null) {
                throw new VelvetException("No key defined");
            }
            return new SortedEntityDef<K, V>(kind, valueClass, (IndexInfo) key, indexes);
        }

        /**
         * Creates a sortable entity definition explicitly specifying the primary key.
         *
         * @param keyClass    primary key class
         * @param <K>         primary key class
         * @param keyFunction function returning primary key from an entity
         * @return entity definition
         */
        public <K extends Comparable<? super K>> ISortedEntityDef<K, V> makeSorted(Class<K> keyClass, Function<V, K> keyFunction) {
            IndexInfo<K, V> keyIndex = new IndexInfo<>(null, keyClass, keyFunction);
            return new SortedEntityDef<>(kind, valueClass, keyIndex, indexes);
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
         * @param <S> key and value class
         * @return entity definition
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <S extends Comparable<? super S>> ISortedSetEntityDef<S> makeSortedSet() {
            // TODO
            return null; //new SortedSetEntityDef(clazz, kind, indexes);
        }

    }
}
