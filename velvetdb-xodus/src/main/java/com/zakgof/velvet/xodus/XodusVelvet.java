package com.zakgof.velvet.xodus;

import com.zakgof.velvet.ISerializer;
import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.IIndexRequest;
import com.zakgof.velvet.request.IEntityDef;
import com.zakgof.velvet.request.IIndexDef;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple store:        No duplicates     ze(key)       ->  ze(value)
 * Sorted store:        No duplicates     xodus(key)    ->  ze(value)
 * Additional index:    With duplicates   xodus(metric) ->  ze(key)
 */
class XodusVelvet implements IVelvet {

    private final Environment env;
    private final Transaction txn;
    private final ISerializer serializer;

    public XodusVelvet(Environment env, Transaction txn, ISerializer serializer) {
        this.env = env;
        this.txn = txn;
        this.serializer = serializer;
    }

    @Override
    public <K, V> void singlePut(IEntityDef<K, V> entityDef, V value) {
        final Store store = storeForEntity(entityDef);
        put(entityDef, value, store);
    }

    private <K, V> Store storeForEntity(IEntityDef<K, V> entityDef) {
        return env.openStore("s/" + entityDef.kind(), StoreConfig.WITHOUT_DUPLICATES, txn);
    }

    private <K, V, M> Store storeForIndex(IIndexDef<K, V, M> index) {
        return env.openStore("s/" + index.entity().kind() + "/" + index.name(), StoreConfig.WITH_DUPLICATES, txn);
    }

    private <K, V> void put(IEntityDef<K, V> entityDef, V value, Store store) {

        K key = entityDef.keyOf(value);

        if (entityDef.indexes().isEmpty()) {
            putToStore(store, key, entityDef.keyClass(), value, entityDef.valueClass(), entityDef.sorted());
        } else {
            V oldValue = get(entityDef, key, store);
            for (String indexName : entityDef.indexes()) {
                IIndexDef<K, V, ?> index = entityDef.index(indexName);
                if (oldValue != null) {
                    deleteFromIndex(index, key, oldValue);
                }
                putToIndex(index, key, value);
            }
            putToStore(store, key, entityDef.keyClass(), value, entityDef.valueClass(), entityDef.sorted());
        }

    }


    private <K, V, M> void deleteFromIndex(IIndexDef<K, V, M> index, K key, V value) {
        M indexValue = index.getter().apply(value);
        Store store = storeForIndex(index);
        deleteFromIndexStore(store, key, index.entity().keyClass(), indexValue, index.type(), index.entity().sorted());
    }

    private <K, M> void deleteFromIndexStore(Store store, K key, Class<K> keyClass, M indexValue, Class<M> indexClass, boolean keySorted) {
        ByteIterable keyBuffer = serialize(keyClass, key, keySorted);
        ByteIterable indexBuffer = serialize(indexClass, indexValue, true);

        try (Cursor cursor = store.openCursor(txn)) {
            boolean find = cursor.getSearchBoth(indexBuffer, keyBuffer);
            if (find) {
                cursor.deleteCurrent();
            } else {
                // TODO: warn
                System.err.println("Could not found old index value for deletion");
            }
        }
    }

    private <K, V, M> void putToIndex(IIndexDef<K, V, M> index, K key, V value) {
        M indexValue = index.getter().apply(value);
        Store store = storeForIndex(index);
        putToStore(store, indexValue, index.type(), key, index.entity().keyClass(), true);
    }

    private <K, V> void putToStore(Store store, K key, Class<K> keyClass, V value, Class<V> valueClass, boolean keySorted) {
        ByteIterable keyBuffer = serialize(keyClass, key, keySorted);
        ByteIterable valueBuffer = serialize(valueClass, value, false);
        store.put(txn, keyBuffer, valueBuffer);
    }

    @Override
    public <K, V> void multiPut(IEntityDef<K, V> entityDef, Collection<V> values) {
        final Store store = storeForEntity(entityDef);
        for (V value : values) {
            put(entityDef, value, store);
        }
    }

    @Override
    public <K, V> V singleGet(IEntityDef<K, V> entityDef, K key) {
        final Store store = storeForEntity(entityDef);
        return get(entityDef, key, store);
    }

    @Nullable
    private <K, V> V get(IEntityDef<K, V> entityDef, K key, Store store) {
        ByteIterable keyBuffer = serialize(entityDef.keyClass(), key, entityDef.sorted());
        ByteIterable valueBuffer = store.get(txn, keyBuffer);
        return valueBuffer == null ? null : deserialize(valueBuffer, entityDef.valueClass());
    }

    @Override
    public <K, V> Map<K, V> multiGet(EntityDef<K, V> entityDef, Collection<K> keys) {
        final Store store = storeForEntity(entityDef);
        return keys.stream()
                .flatMap(key -> Stream.of(get(entityDef, key, store))
                        .filter(Objects::nonNull)
                        .map(value -> new AbstractMap.SimpleEntry<>(key, value))
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (u, v) -> u, LinkedHashMap::new));
    }

    @Override
    public <K, V> Map<K, V> multiGetAll(EntityDef<K, V> entityDef) {
        final Store store = storeForEntity(entityDef);
        Map<K, V> map = new LinkedHashMap<>();
        try (Cursor cursor = store.openCursor(txn)) {
            while (cursor.getNext()) {
                V value = deserialize(cursor.getValue(), entityDef.valueClass());
                K key = entityDef.keyOf(value);
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public <K, V, M> Map<K, V> multiIndexGet(IIndexRequest<K, V, M> indexRequest) {
        IEntityDef<K, V> entityDef = indexRequest.indexDef().entity();
        Class<M> indexClass = indexRequest.indexDef().type();
        Class<K> keyClass = entityDef.keyClass();
        final Store store = storeForEntity(entityDef);
        final Store indexStore = storeForIndex(indexRequest.indexDef());

        Map<K, V> map = new LinkedHashMap<>();

        try (Cursor cursor = indexStore.openCursor(txn)) {
            while (cursor.getNext()) {
                System.out.println(cursor.getKey() + "   ->   " + cursor.getValue());
            }
        }

        try (Cursor cursor = indexStore.openCursor(txn)) {

            IIndexRequest.IBound<K, V, M> start = indexRequest.descending() ? indexRequest.upper() : indexRequest.lower();
            if (start != null) {
                if (start.index() != null) {
                    ByteIterable indexBi = serialize(indexClass, start.index(), true);
                    if (start.key() != null) {
                        ByteIterable keyBi = serialize(keyClass, start.key(), false);
                        if (!gotoStart(cursor, start.inclusive(), indexRequest.descending(), indexBi, keyBi)) {
                            return map;
                        }
                    } else {
                        if (!gotoStart(cursor, start.inclusive(), indexRequest.descending(), indexBi)) {
                            return map;
                        }
                    }
                }
            } else if (!(indexRequest.descending() ? cursor.getLast() : cursor.getNext())) {
                return map;
            }

            IIndexRequest.IBound<K, V, M> endBound = indexRequest.descending() ? indexRequest.lower() : indexRequest.upper();

            ByteIterable endIndex = endBound != null && endBound.index() != null
                    ? serialize(indexClass, endBound.index(), true) : null;
            ByteIterable endKey = endBound != null && endBound.key() != null
                    ? serialize(keyClass, endBound.key(), false) : null;

            do {

                ByteIterable indexBuffer = cursor.getKey();
                ByteIterable keyBuffer = cursor.getValue();

                if (endIndex != null && endKey == null) {
                    if (endBound.inclusive() && indexBuffer.compareTo(endIndex) * (indexRequest.descending() ? -1 : 1) > 0
                            || !endBound.inclusive() && indexBuffer.compareTo(endIndex) * (indexRequest.descending() ? -1 : 1) >= 0) {
                        break;
                    }
                }

                K key = deserialize(keyBuffer, keyClass);
                V value = get(entityDef, key, store);
                map.put(key, value);
            } while (indexRequest.descending() ? cursor.getPrev() : cursor.getNext());
        }
        return map;
    }

    private boolean gotoStart(Cursor cursor, boolean inclusive, boolean descending, ByteIterable indexBi) {
        return descending ? gotoStartDesc(cursor, inclusive, indexBi) : gotoStartAsc(cursor, inclusive, indexBi);
    }

    private boolean gotoStartAsc(Cursor cursor, boolean inclusive, ByteIterable indexBi) {
        ByteIterable firstKey = cursor.getSearchKeyRange(indexBi);
        if (firstKey == null) {
            return false;
        }
        ByteIterable firstIndex = cursor.getKey();
        while (!inclusive && firstIndex.equals(indexBi)) {
            if (!cursor.getNext()) {
                return false;
            }
            firstIndex = cursor.getKey();
        }
        return true;
    }

    private boolean gotoStartDesc(Cursor cursor, boolean inclusive, ByteIterable indexBi) {
        ByteIterable key = cursor.getSearchKey(indexBi);
        if (key != null) {
            if (inclusive) {
                if (!cursor.getNextNoDup()) {
                    cursor.getLast();
                    return true;
                }
            }
            if (!cursor.getPrevNoDup()) {
                return false;
            }
        } else {
            key = cursor.getSearchKeyRange(indexBi);
            if (key != null) {
                if (!cursor.getPrevNoDup()) {
                    return false;
                }
            } else {
                if (!cursor.getLast()) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean gotoStart(Cursor cursor, boolean inclusive, boolean descending, ByteIterable indexBi, ByteIterable keyBi) {
        return descending ? gotoStartDesc(cursor, inclusive, indexBi, keyBi) : gotoStartAsc(cursor, inclusive, indexBi, keyBi);
    }

    private boolean gotoStartAsc(Cursor cursor, boolean inclusive, ByteIterable indexBi, ByteIterable keyBi) {
        return false;
    }

    private boolean gotoStartDesc(Cursor cursor, boolean inclusive, ByteIterable indexBi, ByteIterable keyBi) {
        return false;
    }

    private <V> V deserialize(ByteIterable bi, Class<V> clazz) {
        byte[] bytes = bi.getBytesUnsafe();
        return serializer.deserialize(new ByteArrayInputStream(bytes), clazz);
    }

    private <T> ByteIterable serialize(Class<T> clazz, T object, boolean keySorted) {
        if (keySorted) {
            return KeyUtil.serialize(object);
        } else {
            byte[] bytes = serializer.serialize(object, clazz);
            return new ArrayByteIterable(bytes);
        }
    }

}
