package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.annimon.stream.function.BiConsumer;
import com.zakgof.tools.generic.Pair;

public abstract class AVelvet implements IVelvet {

    // TODO
    @Override
    public <K, V> IStore<K, V> storeWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore) {
        return store(kind, keyClass, valueClass, stores);
    }

    // TODO
    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStoreWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore) {
        return sortedStore(kind, keyClass, valueClass, stores);
    }


    public static abstract class AStore<K, V> implements IStore<K, V> {

        @Override
        public Map<K, V> batchGet(List<K> keys) {
            return Stream.of(keys).distinct().collect(Collectors.toMap(k -> k, k -> get(k), LinkedHashMap::new));
        }

        @Override
        public Map<K, V> getAll() {
            return batchGet(keys());
        }

        // TODO
        static <A, B> void forboth(Collection<A> collA, Collection<B> collB, BiConsumer<A, B> action) {
            Iterator<A> itA = collA.iterator();
            Iterator<B> itB = collB.iterator();
            while(itA.hasNext()) {
                action.accept(itA.next(), itB.next());
            }
        }

        @Override
        public void put(List<K> keys, List<V> values) {
            Iterator<K> keyit = keys.iterator();
            Iterator<V> valit = values.iterator();
            while(keyit.hasNext()) {
                put(keyit.next(), valit.next());
            }
        }

        @Override
        public List<K> put(Collection<V> values) {
            return Stream.of(values).map(v -> put(v)).collect(Collectors.toList());
        }

        @Override
        public  void delete(Collection<K> keys) {
            keys.stream().forEach(this::delete);
        }

    }

    public static abstract class ASingleLink<HK, CK> implements ISingleLink<HK, CK> {

        @Override
        public CK key(HK hk) {
            return keys(hk).stream().findFirst().orElse(null);
        }
        // TODO: poor signature, map is bad
        @Override
        public void batchPut(Map<HK, CK> map) {
            throw new UnsupportedOperationException();
        } // TODO

        @Override
        public Map<HK, CK> batchGet(List<HK> hks) {
            return Stream.of(hks).distinct().map(hk -> Pair.create(hk, key(hk))).filter(p -> p.second() != null).collect(Collectors.toMap(p -> p.first(), p -> p.second()));
        }

        @Override
        public void batchDelete(List<HK> map)      {throw new UnsupportedOperationException();} // TODO
    }

    public static abstract class AMultiLink<HK, CK> implements IMultiLink<HK, CK> {
        // batch
        @Override
        public void batchPutM(Map<HK, List<CK>> map)        {throw new UnsupportedOperationException();} // TODO
        @Override
        public Map<HK, List<CK>> batchGetM(List<HK> hks)    {
            // TODO:  no support in backend ? -> to check
            return Stream.of(hks).collect(Collectors.toMap(hk -> hk, hk -> keys(hk)));
        }
        @Override
        public void batchDelete(Map<HK, List<CK>> map)     {throw new UnsupportedOperationException();} // TODO
        @Override
        public void deleteAll(HK hk)                       {throw new UnsupportedOperationException();} // TODO
        @Override
        public void batchDeleteAll(List<HK> map)           {throw new UnsupportedOperationException();} // TODO
    }

}
