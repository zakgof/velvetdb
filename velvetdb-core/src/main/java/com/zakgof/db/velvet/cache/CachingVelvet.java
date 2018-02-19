package com.zakgof.db.velvet.cache;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.tools.generic.Pair;

class CachingVelvet implements IVelvet {

    private static final Object NULL_VALUE = new Object();
    private final IVelvet proxy;
    private Map<String, Cache<?, ?>> cacheContainer;

    CachingVelvet(IVelvet proxy, Map<String, Cache<?, ?>> cacheContainer) {
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
        return new CachedSingleLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK> IMultiLink<HK, CK> multiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        return new CachedMultiLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK extends Comparable<? super CK>> IKeyIndexLink<HK, CK, CK> primaryKeyIndex(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        return proxy.primaryKeyIndex(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK, CV, M extends Comparable<? super M>> IKeyIndexLink<HK, CK, M> secondaryKeyIndex(Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> metricClass, Class<CK> keyClass, IStore<CK, CV> childStore) {
        return proxy.secondaryKeyIndex(hostKeyClass, edgekind, nodeMetric, metricClass, keyClass, childStore);
    }

    private class CachedStore<K, V> implements IStore<K, V> {

        private IStore<K, V> proxyStore;
        private Cache<K, V> cache;

        public CachedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores) {
            this.proxyStore = proxy.storeWithProxy(kind, keyClass, valueClass, stores, this);
            this.cache = cacheFor(kind);
        }

        @Override
        public V get(K key) {
            return fromCache(cache, key, () -> proxyStore.get(key));
        }

        @Override
        public Map<K, V> batchGet(List<K> hks) {
            Map<K, V> hitMap = hks.stream()
                .distinct()
                .map(hk -> Pair.create(hk, cache.getIfPresent(hk)))
                .filter(p -> p.second() != null)
                .collect(Collectors.toMap(Pair::first, Pair::second));
             // hitMap may contain NULL_VALUEs
             List<K> missKeys = hks.stream()
                .filter(hk -> !hitMap.containsKey(hk))
                .collect(Collectors.toList());
             if (!missKeys.isEmpty()) {
                 Map<K, V> missMap = proxyStore.batchGet(missKeys);
                 hitMap.putAll(missMap);
                 cache.putAll(missMap);
                 hks.stream().distinct().filter(hk -> !hitMap.containsKey(hk)).forEach(hk -> ((Cache)cache).put(hk, NULL_VALUE));
             }
             Map<K, V> fixedHitMap = hitMap.entrySet().stream().filter(e -> e.getValue() != NULL_VALUE).collect(Collectors.toMap(Entry::getKey, Entry::getValue, (u,v) -> {throw new VelvetException("Duplicate key");}, LinkedHashMap::new));
             return fixedHitMap;
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
            this.proxyStore = proxy.sortedStoreWithProxy(kind, keyClass, valueClass, stores, this);
            this.cache = cacheFor(kind);
        }

        @Override
        public V get(K key) {
            return fromCache(cache, key, () -> proxyStore.get(key));
        }

        @Override
        public Map<K, V> batchGet(List<K> keys) {
            Map<K, V> hitMap = keys.stream().distinct()
                .map(key -> Pair.create(key, fromCacheIfPresent(cache, key)))
                .filter(p -> p.second() != null)
                .collect(Collectors.toMap(Pair::first, Pair::second));

            List<K> remainingKeys = keys.stream().filter(key -> !hitMap.containsKey(key)).collect(Collectors.toList());
            if (!remainingKeys.isEmpty()) {
                Map<K, V> missMap = proxyStore.batchGet(remainingKeys);
                cache.putAll(missMap);
                hitMap.putAll(missMap);
            }
            return keys.stream().distinct().collect(Collectors.toMap(hk -> hk, hitMap::get, (u,v) -> {throw new VelvetException("Duplicate key");}, LinkedHashMap::new));
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

    private class CachedSingleLink<HK, CK> implements ISingleLink<HK, CK> {
        private ISingleLink<HK, CK> proxyStore;
        private Cache<HK, CK> cache;

        public CachedSingleLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            this.proxyStore = proxy.singleLink(hostKeyClass, childKeyClass, edgekind);
            this.cache = cacheFor(edgekind);
        }

        @Override
        public void put(HK hk, CK ck) {
            proxyStore.put(hk, ck);
            cache.put(hk, ck);
        }

        @Override
        public void delete(HK hk, CK ck) {
            proxyStore.delete(hk, ck);
            cache.invalidate(hk);
        }

        @Override
        public List<CK> keys(HK hk) {
            CK ck = fromCache(cache, hk, () -> toOne(proxyStore.keys(hk)));
            return ck == null ? Collections.emptyList() : Arrays.asList(ck);
        }

        @Override
        public Map<HK, CK> batchGet(List<HK> hks) {
            Map<HK, CK> hitMap = hks.stream()
               .distinct()
               .map(hk -> Pair.create(hk, cache.getIfPresent(hk)))
               .filter(p -> p.second() != null)
               .collect(Collectors.toMap(Pair::first, Pair::second));
            // hitMap may contain NULL_VALUEs
            List<HK> missKeys = hks.stream()
               .filter(hk -> !hitMap.containsKey(hk))
               .collect(Collectors.toList());
            if (!missKeys.isEmpty()) {
                Map<HK, CK> missMap = proxyStore.batchGet(missKeys);
                hitMap.putAll(missMap);
                cache.putAll(missMap);
                hks.stream().distinct().filter(hk -> !hitMap.containsKey(hk)).forEach(hk -> ((Cache)cache).put(hk, NULL_VALUE));
            }
            Map<HK, CK> fixedHitMap = hitMap.entrySet().stream().filter(e -> e.getValue() != NULL_VALUE).collect(Collectors.toMap(Entry::getKey, Entry::getValue, (u,v) -> {throw new VelvetException("Duplicate key");}, LinkedHashMap::new));
            return fixedHitMap;
        }

        private <T> T toOne(List<T> list) {
            return list.stream().findFirst().orElse(null);
        }

        @Override
        public boolean contains(HK hk, CK ck) {
            CK cachedCK = fromCacheIfPresent(cache, hk);
            if (cachedCK !=null) {
                return cachedCK.equals(ck);
            }
            return proxyStore.contains(hk, ck);
        }
    }

    /**
     * Only cache full result lists
     */
    private class CachedMultiLink<HK, CK> implements IMultiLink<HK, CK> {
        private IMultiLink<HK, CK> proxyStore;
        private Cache<HK, List<CK>> cache;

        public CachedMultiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            this.proxyStore = proxy.multiLink(hostKeyClass, childKeyClass, edgekind);
            this.cache = cacheFor(edgekind);
        }

        @Override
        public void put(HK hk, CK ck) {
            proxyStore.put(hk, ck);
            List<CK> currSet = fromCacheIfPresent(cache, hk);
            if (currSet != null) {
                currSet.add(ck);
                cache.put(hk, new ArrayList<>(currSet));
            }
        }

        @Override
        public void delete(HK hk, CK ck) {
            proxyStore.put(hk, ck);
            List<CK> set = fromCacheIfPresent(cache, hk);
            if (set != null) {
                set.remove(ck);
                cache.put(hk, new ArrayList<>(set));
            }
        }

        @Override
        public List<CK> keys(HK hk) {
            return fromCache(cache, hk, () -> proxyStore.keys(hk));
        }

        @Override
        public boolean contains(HK hk, CK ck) {
            List<CK> set = fromCacheIfPresent(cache, hk);
            if (set != null) {
                return set.contains(ck);
            }
            return proxyStore.contains(hk, ck);
        }
    }

    private <KK, VV> VV fromCacheIfPresent(Cache<KK, VV> cache, KK key) {
        VV vv = cache.getIfPresent(key);
        if (vv == NULL_VALUE) {
            return null;
        }
        return vv;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <KK, VV> VV fromCache(Cache<KK, VV> cache, KK key, Callable<VV> getter) {

            VV vv = cache.getIfPresent(key);
             if (vv == NULL_VALUE) {
                 return null;
             } else if (vv == null) {
                 try {
                     vv = getter.call();
                 } catch (Exception e) {
                     throw new VelvetException(e);
                 }
                 if (vv == null) {
                     ((Cache)cache).put(key, NULL_VALUE);
                 } else {
                     cache.put(key, vv);
                 }
             }
             return vv;

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
