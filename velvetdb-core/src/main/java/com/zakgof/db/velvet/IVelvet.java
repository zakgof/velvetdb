package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.tools.generic.Pair;

public interface IVelvet {

    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores);

    // TODO
    default public <K, V> IStore<K, V> storeWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore) {
        return store(kind, keyClass, valueClass, stores);
    }

    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes);

    // TODO
    default public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStoreWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore) {
        return sortedStore(kind, keyClass, valueClass, stores);
    }

    public <HK, CK> ISingleLink<HK, CK> singleLink(Class<HK> hostKeyClass,  Class<CK> childKeyClass, String edgekind);

    public <HK, CK> IMultiLink<HK, CK> multiLink(Class<HK> hostKeyClass,  Class<CK> childKeyClass, String edgekind);

    public <HK, CK extends Comparable<? super CK>> IPriIndexLink<HK, CK> primaryKeyIndex(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind);

    public <HK, CK, CV, M extends Comparable<? super M>> ISecIndexLink<HK, CK, M> secondaryKeyIndex(Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> metricClass, Class<CK> keyClass, IStore<CK, CV> childStore);

    public interface IStore<K, V> {

        V get(K key);

        default Map<K, V> batchGet(List<K> keys) {
            return keys.stream().distinct().collect(Collectors.toMap(k -> k, k -> get(k), (u, v) -> {throw new VelvetException("Duplicate keys");}, LinkedHashMap::new));
        }

        default Map<K, V> getAll() {
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

        List<K> keys();

        boolean contains(K key);

        long size();

        void put(K key, V value);

        K put(V value);

        default void put(List<K> keys, List<V> values) {
            Iterator<K> keyit = keys.iterator();
            Iterator<V> valit = values.iterator();
            while(keyit.hasNext()) {
                put(keyit.next(), valit.next());
            }
        }

        default List<K> put(Collection<V> values) {
            return values.stream().map(v -> put(v)).collect(Collectors.toList());
        }

        void delete(K key);

        default void delete(Collection<K> keys) {
            keys.stream().forEach(this::delete);
        }

        <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name);

        @Deprecated
        byte[] getRaw(K key);
    }

    public interface IStoreIndex<K, M extends Comparable<? super M>> {
        List<K> keys(ISecQuery<K, M> query);
    }

    public interface IStoreIndexDef<M extends Comparable<? super M>, V> {
        public String name();
        public Function<V, M> metric();
        public Class<M> clazz();
    }

    public interface ISortedStore<K extends Comparable<? super K>, V> extends IStore<K, V> {
        List<K> keys(IKeyQuery<K> query);
    }

    public interface ILink<HK, CK> {
        void put(HK hk, CK ck);
        void delete(HK hk, CK ck);
        List<CK> keys(HK hk);
        boolean contains(HK hk, CK ck);
    }

    public interface ISingleLink<HK, CK> extends ILink<HK, CK> {
        // batch
        default CK key(HK hk) {
            return keys(hk).stream().findFirst().orElse(null);
        }
        // TODO: poor signature, map is bad
        default void batchPut(Map<HK, CK> map) {
            throw new UnsupportedOperationException();
        } // TODO

        default Map<HK, CK> batchGet(List<HK> hks) {
            return hks.stream().distinct().map(hk -> Pair.create(hk, key(hk))).filter(p -> p.second() != null).collect(Collectors.toMap(p -> p.first(), p -> p.second()));
        }

        default void batchDelete(List<HK> map)      {throw new UnsupportedOperationException();} // TODO
    }

    public interface IMultiLink<HK, CK> extends ILink<HK, CK> {
        // batch
        default void batchPutM(Map<HK, List<CK>> map)        {throw new UnsupportedOperationException();} // TODO
        default Map<HK, List<CK>> batchGetM(List<HK> hks)    {
            // TODO:  no support in backend ? -> to check
            return hks.stream().collect(Collectors.toMap(hk -> hk, hk -> keys(hk)));
        }
        default void batchDelete(Map<HK, List<CK>> map)     {throw new UnsupportedOperationException();} // TODO
        default void deleteAll(HK hk)                       {throw new UnsupportedOperationException();} // TODO
        default void batchDeleteAll(List<HK> map)           {throw new UnsupportedOperationException();} // TODO
    }

    public interface IPriIndexLink<HK, CK extends Comparable<? super CK>> extends IMultiLink<HK, CK> {
        List<CK> keys(HK hk, IKeyQuery<CK> query);
    }

    public interface ISecIndexLink<HK, CK, M extends Comparable<? super M>> extends IMultiLink<HK, CK> {

        void update(HK hk, CK ck);

        List<CK> keys(HK hk, ISecQuery<CK, M> query);
    }
}
