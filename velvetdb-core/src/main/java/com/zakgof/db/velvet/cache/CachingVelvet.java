package com.zakgof.db.velvet.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.tools.generic.Pair;

public class CachingVelvet implements IVelvet {

    private final IVelvet proxy;
    private Map<String, Cache<?, ?>> cacheContainer;

    public CachingVelvet(IVelvet proxy, Map<String, Cache<?, ?>> cacheContainer) {
        this.proxy = proxy;
        this.cacheContainer = cacheContainer;
    }

    @Override
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new CachedStore<>(kind, keyClass, valueClass, indexes);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new CachedSortedStore<>(kind, keyClass, valueClass, indexes);
    }

    @Override
    public <HK, CK> ISingleLink<HK, CK> singleLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <HK, CK> IMultiLink<HK, CK> multiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <HK, CK extends Comparable<? super CK>> IKeyIndexLink<HK, CK, CK> primaryKeyIndex(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <HK, CK, CV, M extends Comparable<? super M>> IKeyIndexLink<HK, CK, M> secondaryKeyIndex(Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> metricClass, Class<CK> keyClass, IStore<CK, CV> childStore) {
        // TODO Auto-generated method stub
        return null;
    }

    private class CachedStore<K, V> implements IStore<K, V> {

        private IStore<K, V> proxyStore;
        private Cache<K, V> cache;

        public CachedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores) {
            this.proxyStore = proxy.store(kind, keyClass, valueClass, stores);
            this.cache = cacheFor(kind);
        }

        @Override
        public V get(K key) {
            return fromCache(cache, key, () -> proxyStore.get(key));
        }

        @Override
        public Map<K, V> batchGet(List<K> keys) {
            Map<K, V> hitMap = keys.stream().distinct()
                .map(key -> Pair.create(key, cache.getIfPresent(key)))
                .filter(p -> p.second() != null)
                .collect(Collectors.toMap(Pair::first, Pair::second));

            List<K> remainingKeys = keys.stream().filter(key -> !hitMap.containsKey(key)).collect(Collectors.toList());
            Map<K, V> missMap = proxyStore.batchGet(remainingKeys);
            cache.putAll(missMap);
            hitMap.putAll(missMap);
            return hitMap;
        }

        @Override
        public List<K> keys() {
            // TODO
            return proxyStore.keys();
        }

        @Override
        public boolean contains(K key) {
            return proxyStore.contains(key);
        }

        @Override
        public long size() {
            return proxyStore.size();
        }

        @Override
        public void put(K key, V value) {
            proxyStore.put(key, value);
            cache.put(key, value);
        }

        @Override
        public K put(V value) {
            K key = proxyStore.put(value);
            cache.put(key, value);
            return key;
        }

        @Override
        public void delete(K key) {
            proxyStore.delete(key);
            cache.invalidate(key);
        }

        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            return proxyStore.index(name);
        }

        @Override
        public byte[] getRaw(K key) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private class CachedSortedStore<K extends Comparable<? super K>, V> implements ISortedStore<K, V> {

        private ISortedStore<K, V> proxyStore;
        private Cache<K, V> cache;

        public CachedSortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores) {
            this.proxyStore = proxy.sortedStore(kind, keyClass, valueClass, stores);
            this.cache = cacheFor(kind);
        }

        @Override
        public V get(K key) {
            return fromCache(cache, key, () -> proxyStore.get(key));
        }

        @Override
        public Map<K, V> batchGet(List<K> keys) {
            Map<K, V> hitMap = keys.stream().distinct()
                .map(key -> Pair.create(key, cache.getIfPresent(key)))
                .filter(p -> p.second() != null)
                .collect(Collectors.toMap(Pair::first, Pair::second));

            List<K> remainingKeys = keys.stream().filter(key -> !hitMap.containsKey(key)).collect(Collectors.toList());
            Map<K, V> missMap = proxyStore.batchGet(remainingKeys);
            cache.putAll(missMap);
            hitMap.putAll(missMap);
            return hitMap;
        }

        @Override
        public List<K> keys() {
            // TODO
            return proxyStore.keys();
        }

        @Override
        public boolean contains(K key) {
            return proxyStore.contains(key);
        }

        @Override
        public long size() {
            return proxyStore.size();
        }

        @Override
        public void put(K key, V value) {
            proxyStore.put(key, value);
            cache.put(key, value);
        }

        @Override
        public K put(V value) {
            K key = proxyStore.put(value);
            cache.put(key, value);
            return key;
        }

        @Override
        public void delete(K key) {
            proxyStore.delete(key);
            cache.invalidate(key);
        }

        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            return proxyStore.index(name);
        }

        @Override
        public byte[] getRaw(K key) {
            return null;
        }

        @Override
        public List<K> keys(IRangeQuery<K, K> query) {
            return proxyStore.keys(query);
        }
    }

    private <KK, VV> VV fromCache(Cache<KK, VV> cache, KK key, Callable<VV> getter) {
        try {
            return cache.get(key, getter);
        } catch (ExecutionException e) {
            throw new VelvetException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> Cache<K, V> cacheFor(String kind) {
        return (Cache<K, V>) cacheContainer.computeIfAbsent(kind, k -> CacheBuilder
          .newBuilder()
          .softValues()
          .recordStats()
          .build());
    }

}
