package com.zakgof.db.velvet.mapdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.zakgof.db.velvet.query.IQueryAnchor;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.serialize.ISerializer;

/**
 * normal store: #k/kind1 sorted store: treemap #n/kind : [key] -> [value]
 * 
 * single link: hash map: [key1] -> [key2] 
 * multi link : tree set: [key1, key2] 
 * pri-multilink: set [key1, key2(index)]
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
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass,
            Collection<IStoreIndexDef<?, V>> stores) {
        return new SimpleStore<>(kind); // TODO: indexes
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass,
            Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedStore<>(kind); // TODO: indexes
    }

    abstract class AStore<K, V, MAP extends Map<K, V>> implements IStore<K, V> {

        MAP valueMap;
        private String kind;

        abstract MAP createMap(String kind);

        public AStore(String kind) {
            this.valueMap = createMap(kind);
            this.kind = kind;
        }

        @Override
        public V get(K key) {
            return valueMap.get(key);
        }

        @Override
        public void put(K key, V value) {
            valueMap.put(key, value);
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
    }

    /*
     * hashmap #n/kind : [key] -> [value]
     */
    private class SimpleStore<K, V> extends AStore<K, V, HTreeMap<K, V>> {

        public SimpleStore(String kind) {
            super(kind);
        }

        @Override
        HTreeMap<K, V> createMap(String kind) {
            return db.hashMap("#n/" + kind);
        }

        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            // TODO
            return null;
        }
    }

    /*
     * treemap #n/kind : [key] -> [value]
     */
    private class SortedStore<K extends Comparable<? super K>, V> extends AStore<K, V, BTreeMap<K, V>>
            implements ISortedStore<K, V> {

        public SortedStore(String kind) {
            super(kind);
        }

        @Override
        BTreeMap<K, V> createMap(String kind) {
            return db.treeMap("#n/" + kind);
        }

        @Override
        public List<K> keys(IRangeQuery<K, K> query) {
            return new SortedStoreRequest().go(query);
        }

        /**
         * treemap [key (index)] -> [value]
         */
        private class SortedStoreRequest extends AIndexedRequest<K, K> {

            Entry<K, ?> cursor;

            @Override
            K indexValue() {
                return cursor.getKey();
            }

            @Override
            K get() {
                return cursor.getKey();
            }

            @Override
            boolean check() {
                return cursor != null;
            }

            @Override
            void gotoLowElement(IQueryAnchor<K, K> anchor) {
                if (anchor == null) {
                    cursor = valueMap.firstEntry();
                } else {
                    cursor = anchor.isIncluding() ? valueMap.ceilingEntry(anchor.getMetric())
                            : valueMap.higherEntry(anchor.getMetric());
                }
            }

            @Override
            void gotoHighElement(IQueryAnchor<K, K> anchor) {
                if (anchor == null) {
                    cursor = valueMap.lastEntry();
                } else {
                    cursor = anchor.isIncluding() ? valueMap.floorEntry(anchor.getMetric())
                            : valueMap.lowerEntry(anchor.getMetric());
                }
            }

            @Override
            boolean next() {
                Entry<K, ?> newCursor = valueMap.higherEntry(cursor.getKey());
                if (newCursor != null)
                    cursor = newCursor;
                return newCursor != null;
            }

            @Override
            boolean prev() {
                Entry<K, ?> newCursor = valueMap.lowerEntry(cursor.getKey());
                if (newCursor != null)
                    cursor = newCursor;
                return newCursor != null;
            }

        }

        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            // TODO
            return null;
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

    private abstract class AIndexedRequest<K, M extends Comparable<? super M>> {

        List<K> go(IRangeQuery<K, M> query) {

            List<K> result = new ArrayList<>();

            if (query.isAscending()) {
                IQueryAnchor<K, M> lowAnchor = query.getLowAnchor();
                gotoLowElement(lowAnchor);
                int[] i = new int[] { 0 };
                forwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                forwardWhile(
                        () -> (isBelow(query.getHighAnchor(), true) && (query.getLimit() < 0 || result.size() < query.getLimit())),
                        () -> result.add(get()));
                
                if (check() && query.getHighAnchor() != null && query.getHighAnchor().isIncluding() && get()!=null && get().equals(query.getHighAnchor().getKey()))
                    result.add(get());
            } else {
                IQueryAnchor<K, M> highAnchor = query.getHighAnchor();
                gotoHighElement(highAnchor);
                int[] i = new int[] { 0 };
                backwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                backwardWhile(
                        () -> (isBelow(query.getLowAnchor(), false)
                                && (query.getLimit() < 0 || result.size() < query.getLimit())),
                        () -> result.add(get()));
                if (check() && query.getLowAnchor() != null && query.getLowAnchor().isIncluding() && get()!=null && get().equals(query.getLowAnchor().getKey()))
                    result.add(get());
            }

            return result;
        }

        void forwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                if (!next())
                    break;
            }
        }

        void backwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                if (!prev())
                    break;
            }
        }

        boolean isBelow(IQueryAnchor<K, M> anchor, boolean below) {
            if (anchor == null)
                return true;
            else {
                M anchorValue = anchor.getMetric();
                if (anchorValue == null) {
                    return !get().equals(anchor.getKey());
                }
                int compare = indexValue().compareTo(anchorValue);
                if (compare == 0 && anchor.isIncluding())
                    return true;
                return compare < 0 && below || compare > 0 && !below; // TODO // XOR
            }
        }

        abstract M indexValue();

        abstract K get();

        abstract boolean check();

        abstract void gotoLowElement(IQueryAnchor<K, M> lowAnchor);

        abstract void gotoHighElement(IQueryAnchor<K, M> highAnchor);

        abstract boolean next();

        abstract boolean prev();
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

        abstract class ALinkRequest<M extends Comparable<? super M>> extends AIndexedRequest<K, M> {

            Object[] cursor;

            abstract Object[] anchorElement(M key);

            @SuppressWarnings("unchecked")
            @Override
            M indexValue() {
                return (M) cursor[1];
            }

            @Override
            boolean check() {
                return cursor != null && cursor[0].equals(key1);
            }

            @Override
            void gotoLowElement(IQueryAnchor<K, M> anchor) {
                if (anchor == null) {
                    cursor = connectSet.isEmpty() ? null : connectSet.first();
                } else {
                    M metric = anchor.getMetric();
                    if (metric == null) {
                        Object[] keyEl = el(anchor.getKey());
                        cursor = anchor.isIncluding() ? connectSet.ceiling(keyEl) : connectSet.higher(keyEl);
                    } else {
                        Object[] anchorEl = anchorElement(metric);
                        cursor = connectSet.higher(anchorEl);
                        if (anchor.isIncluding()) {
                            Object[] good = cursor;
                            for(;;) {
                              cursor = cursor == null ? connectSet.last() : connectSet.lower(cursor);
                              if (check() && indexValue().equals(metric))
                                  good = cursor;
                              else
                                  break;
                            }
                            cursor = good;
                        }
                    }
                }
            }

            @Override
            void gotoHighElement(IQueryAnchor<K, M> anchor) {
                if (anchor == null) {
                    cursor = connectSet.last();
                } else {
                    M metric = anchor.getMetric();
                    if (metric == null) {
                        Object[] keyEl = el(anchor.getKey());
                        cursor = anchor.isIncluding() ? connectSet.floor(keyEl) : connectSet.lower(keyEl);
                    } else {
                        Object[] anchorEl = anchorElement(metric);
                        cursor = connectSet.floor(anchorEl);
                        if (!anchor.isIncluding())
                            while(check() && indexValue().equals(metric))
                                cursor = connectSet.lower(cursor);
                    }
                    
                }
            }

            @Override
            boolean next() {
                Object[] newCursor = connectSet.higher(cursor);
                if (newCursor != null)
                    cursor = newCursor;
                return newCursor != null;
            }

            @Override
            boolean prev() {
                Object[] newCursor = connectSet.lower(cursor);
                if (newCursor != null)
                    cursor = newCursor;
                return newCursor != null;
            }
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
     * set [key1, key2(index)]
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
            return new PriIndexRequest().go(query);
        }

        private class PriIndexRequest extends ALinkRequest<K> {

            @Override
            K get() {
                return indexValue();
            }

            @Override
            Object[] anchorElement(K key) {
                return el(key);
            }
        }
    }

    /**
     * treeset [key1, index, key2]
     */
    private class SecMultiLink<K, V, M extends Comparable<? super M>> extends AMultiLink<K>
            implements IKeyIndexLink<K, M> {

        private Function<K, M> keyMetric;

        public SecMultiLink(Object key1, String edgeKind, Function<V, M> nodeMetric, Class<K> keyClass,
                IStore<K, V> childStore) {
            super(key1, edgeKind);
            // TODO: which store ?? store compatibility.
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
            return new SecIndexRequest().go(query);
        }

        private class SecIndexRequest extends ALinkRequest<M> {

            @SuppressWarnings("unchecked")
            @Override
            K get() {
                return (K) cursor[2];
            }

            @Override
            Object[] anchorElement(M val) {
                return new Object[] { key1, val, null };
            }
        }

    }

}
