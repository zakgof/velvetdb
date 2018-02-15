package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        default List<V> get(List<K> keys) {
            return keys.stream().map(k -> get(k)).collect(Collectors.toList());
        }

        default List<V> getAll() {
            return get(keys());
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
