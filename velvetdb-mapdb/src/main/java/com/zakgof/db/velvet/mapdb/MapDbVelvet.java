package com.zakgof.db.velvet.mapdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.IQueryAnchor;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.serialize.ISerializer;

/**
 * normal store: #k/kind1 
 * sorted store: treemap #n/kind : [key] -> [value]
 * 
 * single link: hash map: [key1] -> [key2] 
 * store index: treeset [metric1, key1]
 * multi link : treeset: [key1, key2] 
 * pri-multilink: treeset [key1, key2]
 * sec-multilink: treeset [key1, metric2, key2]
 *
 */
public class MapDbVelvet implements IVelvet {

    private DB db;
    private Supplier<ISerializer> serializerSupplier;

    public MapDbVelvet(DB db, Supplier<ISerializer> serializerSupplier) {
        this.db = db;
        this.serializerSupplier = serializerSupplier;
    }

    @Override
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SimpleStore<>(kind, indexes);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass,
            Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedStore<>(kind, indexes);
    }

    abstract class AStore<K, V, MAP extends Map<K, V>> implements IStore<K, V> {

        MAP valueMap;
        private String kind;
        private Map<String, StoreIndex<K, ?, V>> indexes;

        abstract MAP createMap(String kind);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public AStore(String kind, Collection<IStoreIndexDef<?, V>> indexes) {
            this.valueMap = createMap(kind);
            this.kind = kind;
            this.indexes = indexes.stream().collect(Collectors.toMap(IStoreIndexDef::name, index -> new StoreIndex(kind, index)));
        }

        @Override
        public V get(K key) {
            return valueMap.get(key);
        }

        @Override
        public void put(K key, V value) {
            V oldValue = valueMap.get(key);
            valueMap.put(key, value);
            // remove indexes
            if (oldValue != null) {
                indexes.values().stream().forEach(req -> req.remove(key, oldValue));
            }
            // update indexes
            indexes.values().stream().forEach(req -> req.add(key, value));
        }

        @SuppressWarnings("unchecked")
        @Override
        public K put(V value) {
            Atomic.Long lastKey = db.atomicLong("#auto-" + kind);
            K key = (K) (Long) lastKey.incrementAndGet();
            put(key, value);
            return key;
        }

        @Override
        public void delete(K key) {
            V oldValue = valueMap.get(key);
            if (oldValue != null) {
              indexes.values().stream().forEach(req -> req.remove(key, oldValue));
            }
            valueMap.remove(key);
        }

        @Override
        public List<K> keys() {
            return new ArrayList<>(valueMap.keySet());
        }

        @Override
        public boolean contains(K key) {
            return valueMap.containsKey(key);
        }

        @Override
        public long size() {
            return valueMap.size();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            return (IStoreIndex<K, M>) indexes.get(name);
        }
    }

    private abstract class ARangeQueryProcessor<K, M extends Comparable<? super M>, CURSOR> {
        
        CURSOR cursor;
        protected NavigableSet<CURSOR> set;

        public ARangeQueryProcessor(NavigableSet<CURSOR> set) {
            this.set = set;
        }

        List<K> go(IRangeQuery<K, M> query) {
    
            List<K> result = new ArrayList<>();
    
            if (query.isAscending()) {
                IQueryAnchor<K, M> lowAnchor = query.getLowAnchor();
                gotoLowElement(lowAnchor);
                int[] i = new int[] { 0 };
                forwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                forwardWhile(
                        () -> (isBelow(query.getHighAnchor(), true) && (query.getLimit() < 0 || result.size() < query.getLimit())),
                        () -> result.add(cursorResult()));
                
                if (check() && query.getHighAnchor() != null && query.getHighAnchor().isIncluding() && cursorResult()!=null && cursorResult().equals(query.getHighAnchor().getKey()))
                    result.add(cursorResult());
            } else {
                IQueryAnchor<K, M> highAnchor = query.getHighAnchor();
                gotoHighElement(highAnchor);
                int[] i = new int[] { 0 };
                backwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                backwardWhile(
                        () -> (isBelow(query.getLowAnchor(), false)
                                && (query.getLimit() < 0 || result.size() < query.getLimit())),
                        () -> result.add(cursorResult()));
                if (check() && query.getLowAnchor() != null && query.getLowAnchor().isIncluding() && cursorResult()!=null && cursorResult().equals(query.getLowAnchor().getKey()))
                    result.add(cursorResult());
            }
            return result;
        }
    
        void forwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                CURSOR next = set.higher(cursor);
                if (next == null)
                    break;
                cursor = next;
            }
        }
    
        void backwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                CURSOR prev = set.lower(cursor);
                if (prev == null)
                    break;
                cursor = prev;
            }
        }
    
        boolean isBelow(IQueryAnchor<K, M> anchor, boolean below) {
            if (anchor == null)
                return true;
            else {
                M anchorValue = anchor.getMetric();
                if (anchorValue == null) {
                    return !cursorResult().equals(anchor.getKey());
                }
                int compare = cursorMetric().compareTo(anchorValue);
                if (compare == 0 && anchor.isIncluding())
                    return true;
                return compare < 0 && below || compare > 0 && !below; // TODO // XOR
            }
        }
    
        void gotoLowElement(IQueryAnchor<K, M> anchor) {
            if (anchor == null) {
                cursor = set.isEmpty() ? null : set.first();
            } else {
                M metric = anchor.getMetric();
                if (metric == null) {
                    CURSOR keyEl = cursorAtKey(anchor.getKey());
                    cursor = anchor.isIncluding() ? set.ceiling(keyEl) : set.higher(keyEl);
                } else {
                    CURSOR anchorEl = cursorAfterMetric(metric);
                    cursor = set.higher(anchorEl);
                    if (anchor.isIncluding()) {
                        CURSOR good = cursor;
                        for (;;) {
                            cursor = cursor == null ? (set.isEmpty() ? null : set.last()) : set.lower(cursor);
                            if (check() && cursorMetric().equals(metric))
                                good = cursor;
                            else
                                break;
                        }
                        cursor = good;
                    }
                }
            }
        }
    
        void gotoHighElement(IQueryAnchor<K, M> anchor) {
            if (anchor == null) {
                cursor = set.isEmpty() ? null : set.last();
            } else {
                M metric = anchor.getMetric();
                if (metric == null) {
                    CURSOR keyEl = cursorAtKey(anchor.getKey());
                    cursor = anchor.isIncluding() ? set.floor(keyEl) : set.lower(keyEl);
                } else {
                    CURSOR anchorEl = cursorAfterMetric(metric);
                    cursor = set.floor(anchorEl);
                    if (!anchor.isIncluding())
                        while(check() && cursorMetric().equals(metric))
                            cursor = set.lower(cursor);
                }
                
            }
        }
    
        abstract CURSOR cursorAtKey(K key);
    
        abstract CURSOR cursorAfterMetric(M metric);
    
        abstract M cursorMetric();
    
        abstract K cursorResult();
    
        abstract boolean check();
    }

    /*
     * hashmap #n/kind : [key] -> [value]
     */
    private class SimpleStore<K, V> extends AStore<K, V, HTreeMap<K, V>> {

        public SimpleStore(String kind, Collection<IStoreIndexDef<?, V>> indexes) {
            super(kind, indexes);
        }

        @Override
        HTreeMap<K, V> createMap(String kind) {
            return db.hashMap("#n/" + kind);
        }
    }

    /*
     * treemap #n/kind : [key] -> [value]
     */
    private class SortedStore<K extends Comparable<? super K>, V> extends AStore<K, V, BTreeMap<K, V>>
            implements ISortedStore<K, V> {

        public SortedStore(String kind, Collection<IStoreIndexDef<?, V>> indexes) {
            super(kind, indexes);
        }

        @Override
        BTreeMap<K, V> createMap(String kind) {
            return db.treeMap("#n/" + kind);
        }

        @Override
        public List<K> keys(IRangeQuery<K, K> query) {
            return new SortedStoreRequest(valueMap).go(query);
        }

        /**
         * treemap [key] -> [value]
         */
        private class SortedStoreRequest extends ARangeQueryProcessor<K, K, K> {

            public SortedStoreRequest(BTreeMap<K, V> valueMap) {
                super(valueMap.keySet());
            }

            @Override
            K cursorAtKey(K key) {
                throw new VelvetException("TODO");
            }

            @Override
            K cursorAfterMetric(K metric) {
                return metric;
            }

            @Override
            K cursorMetric() {
                return cursor;
            }

            @Override
            K cursorResult() {
                return cursor;
            }

            @Override
            boolean check() {
                return cursor != null;
            }

        }
    }

    private class StoreIndex<K, M extends Comparable<? super M>, V> implements IStoreIndex<K, M> {

        private NavigableSet<Object[]> indexSet;
        private Function<V, M> metric;

        public StoreIndex(String kind, IStoreIndexDef<M, V> index) {
            
            metric = index.metric();
            indexSet = db.treeSet("#s/" + kind + "/" + index.name(), BTreeKeySerializer.ARRAY2);
        }

        public void add(K key, V value) {
            indexSet.add(new Object[] {metric.apply(value), key});
        }

        public void remove(K key, V value) {
            indexSet.remove(new Object[] {metric.apply(value), key});
        }

        @Override
        public List<K> keys(IRangeQuery<K, M> query) {
            return new StoreIndexProcessor<K, M>(indexSet).go(query);
        }

    }

    /**
     * store index: treeset [metric1, key1]
     */
    private class StoreIndexProcessor<K, M extends Comparable<? super M>> extends ARangeQueryProcessor<K, M, Object[]> {
        
        public StoreIndexProcessor(NavigableSet<Object[]> set) {
            super(set);
        }

        @Override
        Object[] cursorAtKey(K key) {
            throw new VelvetException("TODO");
        }

        @Override
        Object[] cursorAfterMetric(M metric) {
            return new Object[] { metric, null };
        }

        @SuppressWarnings("unchecked")
        @Override
        M cursorMetric() {
            return (M) cursor[0];
        }

        @SuppressWarnings("unchecked")
        @Override
        K cursorResult() {
            return (K) cursor[1];
        }

        @Override
        boolean check() {
            return cursor != null;
        }
    }    
    //
    //
    //
    // public <T> T get(Class<T> clazz, String kind, Object key) {
    // HTreeMap<?, T> valueMap = db.hashMap("#n/" + kind);
    // return valueMap.get(key);
    // }
    //
    // public <K> List<K> allKeys(String kind, Class<K> keyClass) {
    // HTreeMap<K, ?> valueMap = db.hashMap("#n/" + kind);
    // return new ArrayList<>(valueMap.keySet());
    // }
    //
    // public <T> void put(String kind, Object key, T value) {
    // Set<String> kinds = kindsSet();
    // kinds.add(kind);
    // HTreeMap<Object, T> valueMap = db.hashMap("#n/" + kind);
    // valueMap.put(key, value);
    // }
    //
    // public void delete(String kind, Object key) {
    // HTreeMap<Object, ?> valueMap = kvMap(kind);
    // valueMap.remove(key);
    // if (valueMap.isEmpty()) {
    // Set<String> kinds = kindsSet();
    // kinds.remove(kind);
    // }
    //

    @Override
    public <K> ILink<K> simpleIndex(Object key1, String edgekind, LinkType type) {
        if (type == LinkType.Single) {
            return new SingleLink<>(key1, edgekind);
        } else if (type == LinkType.Multi) {
            return new MultiLink<>(key1, edgekind);
        }
        return null;
    }

    @Override
    public <K extends Comparable<? super K>, V> IKeyIndexLink<K, K> primaryKeyIndex(Object key1, String edgekind) {
        return new PriMultiLink<>(key1, edgekind);
    }

    @Override
    public <K, V, M extends Comparable<? super M>> IKeyIndexLink<K, M> secondaryKeyIndex(Object key1, String edgekind,
            Function<V, M> nodeMetric, Class<M> mclazz, Class<K> keyClazz, IStore<K, V> childStore) {
        return new SecMultiLink<K, V, M>(key1, edgekind, nodeMetric, keyClazz, childStore);
    }

    /**
     * hash map: key1 -> key2
     */
    private class SingleLink<K> implements ILink<K> {

        private Object key1;
        private HTreeMap<Object, K> connectMap;

        public SingleLink(Object key1, String edgeKind) {
            this.connectMap = db.hashMap("#s/" + edgeKind);
            this.key1 = key1;
        }

        @Override
        public void put(K key2) {
            connectMap.put(key1, key2);
        }

        @Override
        public void delete(K key2) {
            connectMap.remove(key1, key2);
        }

        @Override
        public List<K> keys(Class<K> clazz) {
            K key = connectMap.get(key1);
            return (key == null) ? Collections.<K> emptyList() : Arrays.asList(key);
        }

        @Override
        public boolean contains(K key2) {
            return key2.equals(connectMap.get(key1));
        }
    }

    private abstract class AMultiLink<K> implements ILink<K> {

        Object key1;
        NavigableSet<Object[]> connectSet;

        public AMultiLink(Object key1, String edgeKind) {
            this.connectSet = db.treeSetCreate("#m/" + edgeKind).serializer(serializer()).makeOrGet();
            this.key1 = key1;
        }

        abstract BTreeKeySerializer<?, ?> serializer();

        abstract Object[] el(K key2);

        @Override
        public void put(K key2) {
            connectSet.add(el(key2));
        }

        @Override
        public void delete(K key2) {
            connectSet.remove(el(key2));
        }

        @Override
        public boolean contains(K key2) {
            return connectSet.contains(el(key2));
        }

    }
    /**
     * tree set [key1, key2]
     */
    private class MultiLink<K> extends AMultiLink<K> {

        public MultiLink(Object key1, String edgeKind) {
            super(key1, edgeKind);
        }

        @Override
        BTreeKeySerializer<?, ?> serializer() {
            return BTreeKeySerializer.ARRAY2;
        }

        @Override
        Object[] el(K key2) {
            return new Object[] { key1, key2 };
        }

        @Override
        public List<K> keys(Class<K> clazz) {
            @SuppressWarnings("unchecked")
            List<K> keys = StreamSupport.stream(Fun.filter(connectSet, key1).spliterator(), false).map(v -> (K) v[1])
                    .collect(Collectors.toList());
            return keys;
        }
    }

    /**
     * set [key1, key2]
     */
    private class PriMultiLink<K extends Comparable<? super K>> extends MultiLink<K> implements IKeyIndexLink<K, K> {

        public PriMultiLink(Object key1, String edgeKind) {
            super(key1, edgeKind);
        }

        @Override
        public void update(K key2) {
            // Do nothin, keys don't change
        }

        @Override
        public List<K> keys(Class<K> clazz, IRangeQuery<K, K> query) {
            return new PriLinkProcessor().go(query);
        }

        private class PriLinkProcessor extends ARangeQueryProcessor<K, K, Object[]> {

            public PriLinkProcessor() {
                super(connectSet);
            }

            @Override
            Object[] cursorAtKey(K key) {
                throw new VelvetException("TODO");
            }

            @Override
            Object[] cursorAfterMetric(K metric) {
                return new Object[] {key1, metric};
            }

            @SuppressWarnings("unchecked")
            @Override
            K cursorMetric() {
                return (K) cursor[1];
            }

            @SuppressWarnings("unchecked")
            @Override
            K cursorResult() {
                return (K) cursor[1];
            }

            @Override
            boolean check() {
                return cursor != null && key1.equals(cursor[0]);
            }

           
        }
    }

    /**
     * treeset [key1, metric2, key2]
     */
    private class SecMultiLink<K, V, M extends Comparable<? super M>> extends AMultiLink<K>
            implements IKeyIndexLink<K, M> {

        private Function<K, M> keyMetric;

        public SecMultiLink(Object key1, String edgeKind, Function<V, M> nodeMetric, Class<K> keyClass,
                IStore<K, V> childStore) {
            super(key1, edgeKind);
            this.keyMetric = key -> {
                V v = childStore.get(key);
                return v == null ? null : nodeMetric.apply(v);
            };
        }

        @Override
        BTreeKeySerializer<?, ?> serializer() {
            return BTreeKeySerializer.ARRAY3;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<K> keys(Class<K> clazz) {
            return connectSet.stream().map(el -> (K) el[2]).collect(Collectors.toList());
        }

        @Override
        public void update(K key2) {
            // TODO
        }

        @Override
        Object[] el(K key2) {
            return new Object[] { key1, keyMetric.apply(key2), key2 };
        }

        @Override
        public List<K> keys(Class<K> clazz, IRangeQuery<K, M> query) {
            return new SecLinkProcessor().go(query);
        }

        private class SecLinkProcessor extends ARangeQueryProcessor<K, M, Object[]> {

            public SecLinkProcessor() {
                super(connectSet);
            }

            @Override
            Object[] cursorAtKey(K key) {
                return new Object[] {key1, keyMetric.apply(key), key};
            }

            @Override
            Object[] cursorAfterMetric(M metric) {
                return new Object[] {key1, metric, null};
            }

            @SuppressWarnings("unchecked")
            @Override
            M cursorMetric() {
                return (M) cursor[1];
            }

            @SuppressWarnings("unchecked")
            @Override
            K cursorResult() {
                return (K) cursor[2];
            }

            @Override
            boolean check() {
                return cursor != null && key1.equals(cursor[0]);
            }

        }

    }

}
