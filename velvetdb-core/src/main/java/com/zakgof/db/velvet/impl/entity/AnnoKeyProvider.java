package com.zakgof.db.velvet.impl.entity;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.annimon.stream.function.Function;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.annotation.Index;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.annotation.SortedKey;
import com.zakgof.db.velvet.properties.AProperty;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.tools.generic.Functions;

public class AnnoKeyProvider<K, V> implements Function<V, K>, IPropertyAccessor<K, V> {

    private Function<V, K> provider;
    private Class<K> keyClass;
    private boolean sorted;
    private Map<String, IProperty<?, V>> propMap = new LinkedHashMap<>();
    private Map<String, IProperty<?, V>> secIndexMap = new LinkedHashMap<>();
    private IProperty<K, V> keyProp;

    @SuppressWarnings("unchecked")
    public AnnoKeyProvider(Class<V> valueClass) {
        for (Field field : getAllFields(valueClass)) {
            field.setAccessible(true);
            if (field.getAnnotation(Key.class) != null || field.getAnnotation(SortedKey.class) != null) {
                keyClass = (Class<K>) field.getType();
                sorted = field.getAnnotation(SortedKey.class) != null;
                provider = value -> {
                    try {
                        return (K) field.get(value);
                    } catch (IllegalAccessException e) {
                        throw new VelvetException(e);
                    }
                };
                keyProp = new FieldProperty<>(field, false);
            } else {
                FieldProperty<Object, V> fieldProperty = new FieldProperty<>(field);
                Index indexAnno = field.getAnnotation(Index.class);
                if (indexAnno != null) {
                    secIndexMap.put(indexAnno.name(), fieldProperty);
                }
                propMap.put(field.getName(), fieldProperty);
            }
        }
        for (Method method : valueClass.getDeclaredMethods()) { // TODO: include inherited!
            method.setAccessible(true);
            if (method.getAnnotation(Key.class) != null || method.getAnnotation(SortedKey.class) != null) {
                keyClass = (Class<K>) method.getReturnType();
                sorted = method.getAnnotation(SortedKey.class) != null;
                provider = (value -> {
                    try {
                        return (K) method.invoke(value);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new VelvetException(e);
                    }
                });
                keyProp = new MethodProperty<>(method);
            } else {
                Index indexAnno = method.getAnnotation(Index.class);
                if (indexAnno != null) {
                    MethodProperty<Object, V> indexProp = new MethodProperty<>(method);
                    secIndexMap.put(indexAnno.name(), indexProp);
                }
            }

        }
        // throw new VelvetException("No annotation for key found in " + valueClass);
    }

    // TODO skip static and transient ?
    static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Field[] declaredFields = type.getDeclaredFields();
        Arrays.sort(declaredFields, Functions.comparator(Field::getName));
        for (Field field : declaredFields) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                fields.add(field);
            }
        }
        if (type.getSuperclass() != null)
            fields.addAll(getAllFields(type.getSuperclass()));
        return fields;
    }

    @Override
    public K apply(V value) {
        return provider.apply(value);
    }

    Class<K> getKeyClass() {
        return keyClass;
    }

    public boolean isSorted() {
        return sorted;
    }

    public boolean hasKey() {
        return provider != null;
    }

    @Override
    public Collection<String> getProperties() {
        return propMap.keySet();
    }

    @Override
    public IProperty<?, V> get(String property) {
        return propMap.get(property);
    }

    @Override
    public IProperty<K, V> getKey() {
        return keyProp;
    }

    public List<IStoreIndexDef<?, V>> getIndexes() {
        return Stream.of(secIndexMap.entrySet()).map(e -> createIndexDefX(e)).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private IStoreIndexDef<?, V> createIndexDefX(Entry<String, IProperty<?, V>> e) {
        return createIndexDef((Map.Entry)e);
    }

    private <M extends Comparable<M>> IStoreIndexDef<M, V> createIndexDef(Map.Entry<String, IProperty<M, V>> entry) {
        return new IStoreIndexDef<M, V>() {
            @Override
            public String name() {
                return entry.getKey();
            }

            @Override
            public Function<V, M> metric() {
                return v -> entry.getValue().get(v);
            }
        };
    }

    private static class FieldProperty<P, V> implements IProperty<P, V> {

        private Field field;
        private boolean settable;

        public FieldProperty(Field field) {
            this(field, true);
        }

        public FieldProperty(Field field, boolean settable) {
            this.field = field;
            this.settable = settable;
        }

        @Override
        public boolean isSettable() {
            return settable;
        }

        @SuppressWarnings("unchecked")
        @Override
        public P get(V instance) {
            try {
                return (P) field.get(instance);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new VelvetException(e);
            }
        }

        @Override
        public V put(V instance, P propValue) {
            try {
                field.set(instance, propValue);
                return instance;
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new VelvetException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<P> getType() {
            return (Class<P>) field.getType();
        }

        @Override
        public String getName() {
            return field.getName();
        }

    }

    private static class MethodProperty<P, V> extends AProperty<P, V> implements IProperty<P, V> {

        private Method method;

        public MethodProperty(Method method) {
            this.method = method;
        }

        @Override
        public boolean isSettable() {
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public P get(V instance) {
            try {
                return (P) method.invoke(instance);
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                throw new VelvetException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<P> getType() {
            return (Class<P>) method.getReturnType();
        }

        @Override
        public String getName() {
            return method.getName();
        }

    }

}