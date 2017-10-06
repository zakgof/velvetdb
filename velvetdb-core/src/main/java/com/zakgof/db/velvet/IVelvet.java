package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.List;
import com.annimon.stream.function.Function;

import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public interface IVelvet {

    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> stores);

    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes);

    public <K> ILink<K> simpleIndex(Object key1, String edgekind, LinkType type);

    public <K extends Comparable<? super K>, T> IKeyIndexLink<K, K> primaryKeyIndex(Object key1, String edgekind);

    public <K, T, M extends Comparable<? super M>> IKeyIndexLink<K, M> secondaryKeyIndex(Object key1, String edgekind, Function<T, M> nodeMetric, Class<M> mclazz, Class<K> keyClazz, IStore<K, T> childStore);

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
    }

    public interface ISortedStore<K extends Comparable<? super K>, V> extends IStore<K, V>, IStoreIndex<K, K> {
    }

    public interface ILink<K> {
        void put(K key2);

        void delete(K key2);

        List<K> keys(Class<K> clazz);

        boolean contains(K key2);
    }

    public interface IKeyIndexLink<K, M extends Comparable<? super M>> extends ILink<K> {
        void update(K key2);

        List<K> keys(Class<K> clazz, IRangeQuery<K, M> query);

        K key(Class<K> clazz, ISingleReturnRangeQuery<K, M> query);
    }

    public abstract class AKeyIndexLink<K, M extends Comparable<? super M>> implements IKeyIndexLink<K, M> {
        @Override
        public K key(Class<K> clazz, ISingleReturnRangeQuery<K, M> query) {
            // TODO
            List<K> keys = keys(clazz, query);
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
