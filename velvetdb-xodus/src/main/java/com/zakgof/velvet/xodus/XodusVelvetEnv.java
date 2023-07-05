package com.zakgof.velvet.xodus;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetReadTransaction;
import com.zakgof.velvet.IVelvetWriteTransaction;
import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.index.IndexDef;
import com.zakgof.velvet.request.IIndexDef;
import com.zakgof.velvet.request.IIndexQuery;
import com.zakgof.velvet.serializer.ISerializer;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple store:        No duplicates     ze(key)       -  ze(value)
 * Sorted store:        No duplicates     xodus(key)    -  ze(value)
 * Additional index:    With duplicates   xodus(metric) -  ze(key)
 */
public class XodusVelvetEnv implements IVelvetEnvironment {

    private final Supplier<ISerializer> serializerFactory;
    private final Environment env;

    public XodusVelvetEnv(String url, Supplier<ISerializer> serializerFactory) {
        this.env = Environments.newInstance(Paths.get(URI.create(url)).toFile());
        this.serializerFactory = serializerFactory;
    }

    @Override
    public <R> R txnRead(Function<IVelvetReadTransaction, R> action) {
        return env.computeInReadonlyTransaction(txn -> action.apply(new XodusTransaction(txn, serializerFactory.get())));
    }

    @Override
    public void txnWrite(Consumer<IVelvetWriteTransaction> action) {
        env.executeInTransaction(txn -> action.accept(new XodusTransaction(txn, serializerFactory.get())));
    }

    public void close() {
        env.close();
    }

    @RequiredArgsConstructor
    private class XodusTransaction implements IVelvetReadTransaction, IVelvetWriteTransaction {

        private final Transaction txn;

        private final ISerializer serializer;

        @Override
        public <K, V> V singleGet(EntityDef<K, V> entityDef, K key) {
            final Store store = readStore(entityDef);
            return store == null ? null : get(entityDef, key, store);
        }

        @Override
        public <K, V> Map<K, V> batchGetMap(EntityDef<K, V> entityDef, Collection<K> keys) {
            final Store store = readStore(entityDef);
            if (store == null) {
                return Map.of();
            }
            return keys.stream()
                    .flatMap(key -> Stream.of(get(entityDef, key, store))
                            .filter(Objects::nonNull)
                            .map(value -> new AbstractMap.SimpleEntry<>(key, value))
                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (u, v) -> u, LinkedHashMap::new));
        }

        @Override
        public <K, V> Map<K, V> batchGetAllMap(IEntityDef<K, V> entityDef) {
            final Store store = readStore(entityDef);
            if (store == null) {
                return Map.of();
            }
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
        public <K, V, M> V singleGetIndex(IIndexQuery<K, V, M> indexQuery) {
            Map<K, V> result = batchGetIndexMap(indexQuery);
            if (result.size() > 1) {
                throw new VelvetException("singleIndexGet returned multiple entries");
            }
            return result.isEmpty() ? null : result.values().iterator().next();
        }

        @Override
        public <K, V, I> Map<K, V> batchGetIndexMap(IIndexQuery<K, V, I> indexQuery) {
            SecIndexWalker<K, V, I> walker = new SecIndexWalker<>(indexQuery);
            return walker.walkMap();
        }

        private class SecIndexWalker<K, V, I> {

            private final IIndexQuery<K, V, I> indexQuery;
            private final Function<V, I> indexMapper;

            private final Store indexStore;
            private final Store valueStore;
            private final Class<I> indexClass;
            private final Map<K, V> map;
            private final boolean primary;
            private final IEntityDef<K, V> entityDef;

            private SecIndexWalker(IIndexQuery<K, V, I> indexQuery) {

                IndexDef<K, V, I> indexDef = indexQuery.indexDef();
                IEntityDef<K, V> entityDef = indexDef.entity();

                this.indexQuery = indexQuery;
                this.indexClass = indexDef.type();
                this.entityDef = entityDef;
                this.primary = indexDef.name().isEmpty();
                this.valueStore = readStore(entityDef);
                this.indexStore = primary ? valueStore : readStore(indexDef);
                this.indexMapper = indexDef.getter();
                this.map = new LinkedHashMap<>();
            }

            private Map<K, V> walkMap() {
                if (indexStore == null)
                    return map;
                try (Cursor cursor = indexStore.openCursor(txn)) {
                    ByteIterable[] startBIs = collectBIs(0);
                    if (!gotoStart(cursor, startBIs)) {
                        return map;
                    }
                    ByteIterable[] endBIs = collectBIs(1);
                    boolean endInclusive = isInclusive(1);

                    for (int elements = 0; indexQuery.limit() < 0 || elements < indexQuery.limit(); elements++) {

                        ByteIterable indexBuffer = cursor.getKey();
                        ByteIterable keyBuffer = cursor.getValue();

                        if (endBIs[1] != null) {
                            if (keyBuffer.equals(endBIs[1])) {
                                if (endInclusive) {
                                    append(indexBuffer, keyBuffer);
                                }
                                break;
                            }
                        } else if (endBIs[0] != null) {
                            int comparison = indexBuffer.compareTo(endBIs[0]);
                            if (comparison == 0 && !endInclusive) {
                                break;
                            }
                            if (comparison < 0 && indexQuery.descending()
                                    || comparison > 0 && !indexQuery.descending()) {
                                break;
                            }
                        }

                        append(indexBuffer, keyBuffer);

                        boolean stepResult = indexQuery.descending() ? cursor.getPrev() : cursor.getNext();
                        if (!stepResult)
                            break;
                    }
                }
                return map;
            }

            private void append(ByteIterable indexBI, ByteIterable keyBI) {
                K key = primary ? (K) deserializeXodus(indexBI, indexClass) : deserialize(keyBI, entityDef.keyClass());
                V value = primary ? (V) deserialize(keyBI, entityDef.valueClass()) : get(entityDef, key, valueStore);
                map.put(key, value);
            }

            private boolean isInclusive(int order) {
                IIndexQuery.IBound<K, I> bound = bound(order);
                return bound != null && bound.inclusive();
            }

            private IIndexQuery.IBound<K, I> bound(int order) {
                return indexQuery.bounds()[indexQuery.descending() ? 1 - order : order];
            }

            private ByteIterable[] collectBIs(int order) {
                ByteIterable[] bis = new ByteIterable[2];
                IIndexQuery.IBound<K, I> bound = bound(order);
                if (bound != null) {
                    I index = bound.index();
                    if (bound.key() != null) {
                        V value = get(entityDef, bound.key(), valueStore);
                        if (value == null) {
                            throw new VelvetException("No value exists for index key " + bound.key());
                        }
                        index = indexMapper.apply(value);
                    }
                    if (index != null) {
                        bis[0] = serialize(indexClass, index, true);
                    }
                    if (bound.key() != null) {
                        bis[1] = serialize(entityDef.keyClass(), bound.key(), false);
                    }
                }
                return bis;
            }

            private boolean gotoStart(Cursor cursor, ByteIterable[] startBIs) {
                ByteIterable indexBi = startBIs[0];
                ByteIterable keyBi = startBIs[1];
                boolean inclusive = isInclusive(0);
                if (indexQuery.descending()) {
                    if (indexBi == null) {
                        return cursor.getLast();
                    }
                    if (keyBi == null) {
                        ByteIterable key = cursor.getSearchKey(indexBi);
                        if (key != null) {
                            if (inclusive) {
                                if (!cursor.getNextNoDup()) {
                                    cursor.getLast();
                                    return true;
                                }
                            }
                            return cursor.getPrevNoDup();
                        } else {
                            key = cursor.getSearchKeyRange(indexBi);
                            if (key != null) {
                                return cursor.getPrevNoDup();
                            } else {
                                return cursor.getLast();
                            }
                        }
                    } else {
                        ByteIterable bi = cursor.getSearchBothRange(indexBi, keyBi);
                        if (bi == null) {
                            return false;
                        }
                        if (bi.equals(keyBi) && !inclusive) {
                            return cursor.getPrev();
                        }
                        return true;
                    }
                } else {
                    if (indexBi == null) {
                        return cursor.getNext();
                    }
                    if (keyBi == null) {
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
                    } else {
                        ByteIterable key = cursor.getSearchBothRange(indexBi, keyBi);
                        if (key == null) {
                            return false;
                        }
                        if (key.equals(keyBi) && !inclusive) {
                            return cursor.getNext();
                        }
                        return true;
                    }
                }
            }
        }

        @Override
        public <K, V> void putValue(IEntityDef<K, V> entityDef, V value) {
            final Store store = writeStore(entityDef);
            put(entityDef, value, store);
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

        private <K, V, M> void putToIndex(IIndexDef<K, V, M> index, K key, V value) {
            M indexValue = index.getter().apply(value);
            Store store = writeStore(index);
            putToStore(store, indexValue, index.type(), key, index.entity().keyClass(), true);
        }

        private <K, V> void putToStore(Store store, K key, Class<K> keyClass, V value, Class<V> valueClass, boolean keySorted) {
            ByteIterable keyBuffer = serialize(keyClass, key, keySorted);
            ByteIterable valueBuffer = serialize(valueClass, value, false);
            store.put(txn, keyBuffer, valueBuffer);
        }

        @Override
        public <K, V> void deleteKey(IEntityDef<K, V> entityDef, K key) {
            Store store = writeStore(entityDef);
            delete(entityDef, key, store);
        }

        private <V, K> void delete(IEntityDef<K, V> entityDef, K key, Store store) {
            ByteIterable keyBuffer = serialize(entityDef.keyClass(), key, entityDef.sorted());
            if (!entityDef.indexes().isEmpty()) {
                V oldValue = get(entityDef, key, store);
                for (String indexName : entityDef.indexes()) {
                    IIndexDef<K, V, ?> index = entityDef.index(indexName);
                    if (oldValue != null) {
                        deleteFromIndex(index, key, oldValue);
                    }
                }
            }
            store.delete(txn, keyBuffer);
        }

        private <K, V, M> void deleteFromIndex(IIndexDef<K, V, M> index, K key, V value) {
            M indexValue = index.getter().apply(value);
            Store store = writeStore(index);
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

        private <K, V> Store readStore(IEntityDef<K, V> entityDef) {
            return openStore("s/" + entityDef.kind(), StoreConfig.USE_EXISTING);
        }

        private <K, V, M> Store readStore(IIndexDef<K, V, M> index) {
            return openStore("s/" + index.entity().kind() + "/" + index.name(), StoreConfig.USE_EXISTING);
        }

        private <K, V> Store writeStore(IEntityDef<K, V> entityDef) {
            return openStore("s/" + entityDef.kind(), StoreConfig.WITHOUT_DUPLICATES);
        }

        private <K, V, M> Store writeStore(IIndexDef<K, V, M> index) {
            return openStore("s/" + index.entity().kind() + "/" + index.name(), StoreConfig.WITH_DUPLICATES);
        }

        private <K, V> Store openStore(String name, StoreConfig config) {
            try {
                return env.openStore(name, config, txn);
            } catch (ExodusException e) {
                if (StoreConfig.USE_EXISTING == config && e.getMessage().startsWith("Can't restore meta information for store")) {
                    return null;
                }
                throw new VelvetException(e);
            }
        }

        @Nullable
        private <K, V> V get(IEntityDef<K, V> entityDef, K key, Store store) {
            ByteIterable keyBuffer = serialize(entityDef.keyClass(), key, entityDef.sorted());
            ByteIterable valueBuffer = store.get(txn, keyBuffer);
            return valueBuffer == null ? null : deserialize(valueBuffer, entityDef.valueClass());
        }

        private <V> V deserialize(ByteIterable bi, Class<V> clazz) {
            byte[] bytes = bi.getBytesUnsafe();
            return serializer.deserialize(new ByteArrayInputStream(bytes), clazz);
        }

        private <V> V deserializeXodus(ByteIterable bi, Class<V> clazz) {
            return KeyUtil.deserialize(clazz, bi);
        }

        private <T> ByteIterable serialize(Class<T> clazz, T object, boolean xodus) {
            if (xodus) {
                return KeyUtil.serialize(object);
            } else {
                byte[] bytes = serializer.serialize(object, clazz);
                return new ArrayByteIterable(bytes);
            }
        }
    }
}
