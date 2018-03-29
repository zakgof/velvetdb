package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.annimon.stream.function.BiConsumer;
import com.annimon.stream.function.Function;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISecQuery;

public interface IVelvet {

    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores);

    public <K, V> IStore<K, V> storeWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore);

    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes);

    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStoreWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores, IStore<K, V> parentStore);

    public <HK, CK> ISingleLink<HK, CK> singleLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind);

    public <HK, CK> IMultiLink<HK, CK> multiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind);

    public <HK, CK extends Comparable<? super CK>> IPriIndexLink<HK, CK> primaryKeyIndex(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind);

    public <HK, CK, CV, M extends Comparable<? super M>> ISecIndexLink<HK, CK, M> secondaryKeyIndex(Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> metricClass, Class<CK> keyClass, IStore<CK, CV> childStore);

    public interface IStore<K, V> {

        V get(K key);

        Map<K, V> batchGet(List<K> keys);

        Map<K, V> getAll();

        // TODO
        static <A, B> void forboth(Collection<A> collA, Collection<B> collB, BiConsumer<A, B> action) {
            Iterator<A> itA = collA.iterator();
            Iterator<B> itB = collB.iterator();
            while (itA.hasNext()) {
                action.accept(itA.next(), itB.next());
            }
        }

        List<K> keys();

        boolean contains(K key);

        long size();

        void put(K key, V value);

        K put(V value);

        void put(List<K> keys, List<V> values);

        List<K> put(Collection<V> values);

        void delete(K key);

        void delete(Collection<K> keys);

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
        CK key(HK hk);

        // TODO: poor signature, map is bad
        void batchPut(Map<HK, CK> map); // TODO

        Map<HK, CK> batchGet(List<HK> hks);

        void batchDelete(List<HK> map); // TODO
    }

    public interface IMultiLink<HK, CK> extends ILink<HK, CK> {
        // batch
        void batchPutM(Map<HK, List<CK>> map); // TODO

        Map<HK, List<CK>> batchGetM(List<HK> hks);

        void batchDelete(Map<HK, List<CK>> map); // TODO

        void deleteAll(HK hk); // TODO

        void batchDeleteAll(List<HK> map); // TODO
    }

    public interface IPriIndexLink<HK, CK extends Comparable<? super CK>> extends IMultiLink<HK, CK> {
        List<CK> keys(HK hk, IKeyQuery<CK> query);
    }

    public interface ISecIndexLink<HK, CK, M extends Comparable<? super M>> extends IMultiLink<HK, CK> {

        void update(HK hk, CK ck);

        List<CK> keys(HK hk, ISecQuery<CK, M> query);
    }




}
