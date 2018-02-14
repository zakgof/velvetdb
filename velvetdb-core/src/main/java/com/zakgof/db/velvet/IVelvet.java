package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public interface IVelvet {

    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores);

    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes);

    public <HK, CK> ILink<HK, CK> simpleIndex(HK key1, Class<HK> hostKeyClass,  Class<CK> childKeyClass, String edgekind, LinkType type);

    public <HK, CK extends Comparable<? super CK>> IKeyIndexLink<HK, CK, CK> primaryKeyIndex(HK key1, Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind);

    public <HK, CK, T, M extends Comparable<? super M>> IKeyIndexLink<HK, CK, M> secondaryKeyIndex(HK key1, Class<HK> hostKeyClass, String edgekind, Function<T, M> nodeMetric, Class<M> mclazz, Class<CK> keyClazz, IStore<CK, T> childStore);

    public interface IStore<K, V> {
        V get(K key);

        byte[] getRaw(K key);

        void put(K key, V value);

        K put(V value);

        void delete(K key);

        List<K> keys();

        boolean contains(K key);

        long size();

        <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name);
    }

    public interface IStoreIndex<K, M extends Comparable<? super M>> {
        List<K> keys(IRangeQuery<K, M> query);
    }

    public interface IStoreIndexDef<M extends Comparable<? super M>, V> {
        public String name();
        public Function<V, M> metric();
        public Class<M> clazz();
    }

    public interface ISortedStore<K extends Comparable<? super K>, V> extends IStore<K, V>, IStoreIndex<K, K> {
    }

    public interface ILink<HK, CK> {
        void put(CK key2);

        void delete(CK key2);

        List<CK> keys();

        boolean contains(CK key2);
    }

    public interface IKeyIndexLink<HK, CK, M extends Comparable<? super M>> extends ILink<HK, CK> {

        void update(CK key2);

        List<CK> keys(IRangeQuery<CK, M> query);

        default CK key(ISingleReturnRangeQuery<CK, M> query) {
            // TODO
            List<CK> keys = keys(query);
            if (keys.isEmpty())
                return null;
            if (keys.size() > 1)
                throw new VelvetException("");
            return keys.get(0);
        }
    }

    public enum LinkType {
        Single, Multi,
    }
}
