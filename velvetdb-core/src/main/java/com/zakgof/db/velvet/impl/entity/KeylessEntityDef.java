package com.zakgof.db.velvet.impl.entity;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public class KeylessEntityDef<V> extends SortedEntityDef<Long, V> implements IKeylessEntityDef<V> {

    private final Map<V, Long> keys = Collections.synchronizedMap(new WeakHashMap<>());
    private IPropertyAccessor<Long, V> propertyAccessor;

    public KeylessEntityDef(Class<V> valueClass, String kind, Collection<IStoreIndexDef<?, V>> indexes) {
        super(Long.class, valueClass, kind, null, indexes);
        setKeyProvider(v -> keys.get(v));
        propertyAccessor = isMonolythic(valueClass) ? new MonolythicPropertyProvider(valueClass) : new KeylessPropertyProvider(valueClass);
    }

    private boolean isMonolythic(Class<V> valueClass) {
        return valueClass.isPrimitive() || valueClass == String.class;
    }

    @Override
    public Long keyOf(V value) {
        return keys.get(value);
    }

    @Override
    public V get(IVelvet velvet, Long key) {
        V value = super.get(velvet, key);
        keys.put(value, key);
        return value;
    }

    @Override
    public List<V> get(IVelvet velvet, List<Long> keys) {
        List<V> values = super.get(velvet, keys);
        forboth(keys, values, (k, v) -> this.keys.put(v, k));
        return values;
    }

    @Override
    public List<V> getAll(IVelvet velvet) {
        Map<Long, V> values = store(velvet).getAllAsMap();
        Map<V, Long> map = values.entrySet().stream().collect(Collectors.toMap(Entry::getValue, Entry::getKey));
        this.keys.putAll(map);
        return new ArrayList<>(values.values());
    }

    private <A, B> void forboth(Collection<A> collA, Collection<B> collB, BiConsumer<A, B> action) {
        Iterator<A> itA = collA.iterator();
        Iterator<B> itB = collB.iterator();
        while(itA.hasNext()) {
            action.accept(itA.next(), itB.next());
        }
    }

    @Override
    public Long put(IVelvet velvet, V value) {
        Long key = keys.get(value);
        if (key == null) {
            key = store(velvet).put(value);
        } else {
            store(velvet).put(key, value);
        }
        keys.put(value, key);
        return key;
    }

    @Override
    public List<Long> put(IVelvet velvet, List<V> values) {
        List<Long> keyz = values.stream().map(keys::get).collect(Collectors.toList());
        boolean nullz = keyz.contains(null);
        if (nullz && keyz.stream().anyMatch(k -> k!=null))
            throw new VelvetException("Cannot put both nodes with and without keys in same batch");
        if (nullz) {
            List<Long> newkeyz = store(velvet).put(values);
            Iterator<V> valit = values.iterator();
            Iterator<Long> keyit = newkeyz.iterator();
            while(valit.hasNext()) {
                keys.put(valit.next(), keyit.next());
            }
            return newkeyz;
        } else {
            store(velvet).put(keyz, values);
            return keyz;
        }
    }

    @Override
    public Long put(IVelvet velvet, Long key, V value) {
        // TODO : think over
        /*
        V oldValue = store(velvet).get(key);
        if (!value.equals(oldValue)) {
            keys.remove(oldValue);
        }
        */
        store(velvet).put(key, value);
        keys.put(value, key);
        return key;
    }

    @Override
    public List<Long> put(IVelvet velvet, List<Long> keyz, List<V> values) {
        store(velvet).put(keyz, values);
        Iterator<V> valit = values.iterator();
        Iterator<Long> keyit = keyz.iterator();
        while(valit.hasNext()) {
            keys.put(valit.next(), keyit.next());
        }
        return keyz;
    }

    @Override
    public void deleteValue(IVelvet velvet, V value) {
        super.deleteValue(velvet, value);
        keys.remove(value);
    }

    private class KeylessPropertyProvider extends AnnoKeyProvider<Long, V> implements IPropertyAccessor<Long, V> {

        private IProperty<Long, V> keyProperty;

        public KeylessPropertyProvider(Class<V> valueClass) {
            super(valueClass);
            this.keyProperty = new IProperty<Long, V>() {

                @Override
                public Long get(V instance) {
                    return keyOf(instance);
                }

                @Override
                public Class<Long> getType() {
                    return Long.class;
                }

                @Override
                public String getName() {
                    return "[autokey]";
                }
            };
        }

        @Override
        public IProperty<Long, V> getKey() {
            return keyProperty;
        }

    }

    private class MonolythicPropertyProvider  implements IPropertyAccessor<Long, V> {

        private IProperty<V, V> valueProperty;
        private IProperty<Long, V> keyProperty;

        public MonolythicPropertyProvider(Class<V> valueClass) {
            this.keyProperty = new IProperty<Long, V>() {

                @Override
                public Long get(V instance) {
                    return keyOf(instance);
                }

                @Override
                public Class<Long> getType() {
                    return Long.class;
                }

                @Override
                public String getName() {
                    return "[autokey]";
                }
            };
            valueProperty = new IProperty<V, V>() {

                @Override
                public V get(V instance) {
                    return instance;
                }

                @Override
                public V put(V instance, V propValue) {
                    return propValue;
                }

                @Override
                public Class<V> getType() {
                    return valueClass;
                }

                @Override
                public String getName() {
                    return "value";
                }
            };
        }

        @Override
        public Collection<String> getProperties() {
            return Arrays.asList("value");
        }

        @Override
        public IProperty<?, V> get(String property) {
            return property.equals("value") ? valueProperty  : null;
        }

        @Override
        public IProperty<Long, V> getKey() {
            return keyProperty;
        }

    }

    @Override
    public IPropertyAccessor<Long, V> propertyAccessor() {
        return propertyAccessor;
    }

}
