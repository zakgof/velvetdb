package com.zakgof.db.velvet.xodus;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISecAnchor;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.KeyQueries;
import com.zakgof.db.velvet.query.SecQueries;
import com.zakgof.serialize.ISerializer;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;

/**
 * Simple store:        No duplicates     ze(key)       ->  ze(value)
 * Sorted store:        No duplicates     xodus(key)    ->  ze(value)
 * Additional index:    With duplicates   xodus(metric) ->  ze(key) ??
 *
 * TODO: add index on existing store ?
 *
 * SingleLink:          No duplicates     ze(key1) -> ze(key2)
 * MultiLink:           No duplicates    [ze(key1), ze(key2)] -> empty
 * PriMultiLink:        No duplicates    [ze(key1), xodus(key2)] -> empty
 * SecMultiLink:        With duplicates  [ze(key1), xodus(metric2)] -> key2
 */
class XodusVelvet implements IVelvet {

    private Environment env;
    private Transaction tx;
    private IKeyGen keyGen;
    private Supplier<ISerializer> serializerSupplier;

    XodusVelvet(Environment env, Transaction tx, IKeyGen keyGen, Supplier<ISerializer> serializerSupplier) {
        this.env = env;
        this.tx = tx;
        this.keyGen = keyGen;
        this.serializerSupplier = serializerSupplier;
    }

    @Override
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SimpleStore<>(kind, keyClass, valueClass, indexes);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedStore<>(kind, keyClass, valueClass, indexes);
    }

    private class SimpleStore<K, V> implements IStore<K, V> {

        protected Class<K> keyClass;
        protected Store valueMap;
        protected String kind;
        private Class<V> valueClass;
        private Map<String, StoreIndexProcessor<K, V, ?>> indexes;

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public SimpleStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
            this.kind = kind;
            this.valueMap = store(kind);
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.indexes = indexes.stream().collect(Collectors.toMap(IStoreIndexDef::name, indexDef -> createStoreReq((IStoreIndexDef) indexDef)));
        }

        private <M extends Comparable<? super M>> StoreIndexProcessor<K, V, M> createStoreReq(IStoreIndexDef<M, V> indexDef) {
            return new StoreIndexProcessor<>(keyClass, indexDef.metric(), keyMetric(indexDef), "#s" + kind + "/" + indexDef.name(), this);
        }

        private <M extends Comparable<? super M>> Function<K, M> keyMetric(IStoreIndexDef<M, V> indexDef) {
            return key -> {
                V v = get(key);
                if (v == null) {
                    System.err.println("null value for key " + key + " received during secondary index retrieval ");
                    return null;
                }
                return indexDef.metric().apply(v);
            };
        }

        private Store store(String kind) {
            return env.openStore("#n/" + kind, StoreConfig.WITHOUT_DUPLICATES, tx);
        }

        ByteIterable toBi(K key) {
            return XodusVelvet.this.toBi(key, keyClass);
        }

        K toObj(ByteIterable bi) {
            return XodusVelvet.this.toObj(keyClass, bi);
        }
//
//        @Override
//        public byte[] getRaw(K key) {
//            ByteIterable keyBi = toBi(key);
//            debug(valueMap);
//            ByteIterable valueBi = valueMap.get(tx, keyBi);
//            return valueBi == null ? null : biToByteArray(valueBi);
//        }

        @Override
        public V get(K key) {
            ByteIterable keyBi = toBi(key);
            debug(valueMap);
            ByteIterable valueBi = valueMap.get(tx, keyBi);
            return valueBi == null ? null : XodusVelvet.this.toObj(valueClass, valueBi);
        }

        @Override
        public void put(K key, V value) {
            ByteIterable keyBi = toBi(key);
            ByteIterable valueBi = XodusVelvet.this.toBi(value, valueClass);
            ByteIterable oldValueBi = valueMap.get(tx, keyBi);
            valueMap.put(tx, keyBi, valueBi);
            // remove indexes
            if (oldValueBi != null) {
                V oldValue = XodusVelvet.this.toObj(valueClass, oldValueBi);
                indexes.values().stream().forEach(req -> req.remove(oldValue, key)); // TODO: mutables in cache !!!
            }
            // update indexes
            indexes.values().stream().forEach(req -> req.add(value, key));
        }

        @Override
        public K put(V value) {
            throw new VelvetException("Sorted store should be used for autogenerated keys");
        }

        @Override
        public void delete(K key) {
            ByteIterable keyBi = toBi(key);

            ByteIterable oldValueBi = valueMap.get(tx, keyBi);
            if (oldValueBi != null) {
                V oldValue = XodusVelvet.this.toObj(valueClass, oldValueBi);
                indexes.values().stream().forEach(req -> req.remove(oldValue, key)); // TODO: mutables in cache !!!
            }

            valueMap.delete(tx, keyBi);
        }

        @Override
        public List<K> keys() {

            debug(valueMap);

            try (Cursor cursor = valueMap.openCursor(tx)) {
                List<K> keys = new ArrayList<>((int) valueMap.count(tx));
                while (cursor.getNext())
                    keys.add(toObj(cursor.getKey()));
                return keys;
            }
        }

        @Override
        public boolean contains(K key) {
            ByteIterable keyBi = toBi(key);
            return valueMap.get(tx, keyBi) != null;
        }

        @Override
        public long size() {
            return valueMap.count(tx);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            return ((StoreIndexProcessor<K, V, M>) indexes.get(name));
        }

    }

    private class SortedStore<K extends Comparable<? super K>, V> extends SimpleStore<K, V> implements ISortedStore<K, V> {

        public SortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
            super(kind, keyClass, valueClass, indexes);
        }

        @Override
        ByteIterable toBi(K key) {
            return BytesUtil.keyToBi(key);
        }

        @Override
        K toObj(ByteIterable bi) {
            return BytesUtil.keyBiToObj(keyClass, bi);
        }

        @Override
        public List<K> keys(IKeyQuery<K> query) {
            return new SorterStoreProcessor<>(keyClass).go(valueMap, SecQueries.from(query));
        }

        @Override
        public K put(V value) {
            K key = keyGen.acquire(kind, keyClass, valueMap, tx);
            put(key, value);
            return key;
        }
    }

    private abstract class ARangeQueryProcessor<K, M extends Comparable<? super M>> {

        Cursor cursor;
        boolean cursorValid = true;

        List<K> go(Store store, ISecQuery<K, M> query) {

            debug(store);

            List<K> result = new ArrayList<>();
            try (Cursor c = store.openCursor(tx)) {
                cursor = c;
                if (query.isAscending()) {
                    ISecAnchor<K, M> lowAnchor = query.getLowAnchor();
                    gotoLowElement(lowAnchor);
                    int[] i = new int[] { 0 };
                    forwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                    forwardWhile(() -> (isBelow(query.getHighAnchor(), true) && (query.getLimit() < 0 || result.size() < query.getLimit())), () -> result.add(get()));
                    if (check() && query.getHighAnchor() != null && query.getHighAnchor().isIncluding() && get() != null && get().equals(query.getHighAnchor().getKey()))
                        result.add(get());
                } else {
                    ISecAnchor<K, M> highAnchor = query.getHighAnchor();
                    gotoHighElement(highAnchor);
                    int[] i = new int[] { 0 };
                    backwardWhile(() -> (i[0] < query.getOffset()), () -> i[0]++);
                    backwardWhile(() -> (isBelow(query.getLowAnchor(), false) && (query.getLimit() < 0 || result.size() < query.getLimit())), () -> result.add(get()));
                    if (check() && query.getLowAnchor() != null && query.getLowAnchor().isIncluding() && get() != null && get().equals(query.getLowAnchor().getKey()))
                        result.add(get());
                }
            }
            return result;
        }

        void forwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                if (!cursor.getNext()) {
                    cursorValid = false;
                    break;
                }
            }
        }

        void backwardWhile(Supplier<Boolean> condition, Runnable action) {
            while (check() && condition.get()) {
                action.run();
                if (!cursor.getPrev()) {
                    cursorValid = false;
                    break;
                }
            }
        }

        boolean isBelow(ISecAnchor<K, M> anchor, boolean below) {
            if (anchor == null)
                return true;
            else {
                M m = anchor.getMetric();
                if (m == null) {
                    return !get().equals(anchor.getKey());
                }
                M indexValue = indexValue();
                if (indexValue == null)
                    return false;
                int compare = indexValue.compareTo(m);
                if (compare == 0 && anchor.isIncluding())
                    return true;
                return compare < 0 && below || compare > 0 && !below; // TODO XOR
            }
        }

        abstract M indexValue();

        abstract K get();

        abstract boolean check();

        abstract void gotoLowElement(ISecAnchor<K, M> lowAnchor);

        abstract void gotoHighElement(ISecAnchor<K, M> highAnchor);
    }

    private abstract class AStoreProcessor<K, M extends Comparable<? super M>> extends ARangeQueryProcessor<K, M> {

        protected Class<K> keyClass;
        private Function<K, M> keyMetric;

        AStoreProcessor(Class<K> keyClass, Function<K, M> keyMetric) {
            this.keyClass = keyClass;
            this.keyMetric = keyMetric;
        }

        @Override
        K get() {
            return BytesUtil.keyBiToObj(keyClass, cursor.getKey());
        }

        @Override
        boolean check() {
            ByteIterable key = cursor.getKey();
            return cursorValid && key.getLength() != 0;
        }

        @Override
        void gotoLowElement(ISecAnchor<K, M> anchor) {
            if (anchor == null) {
                cursor.getNext();
            } else {
                M metric = anchor.getMetric();
                if (metric == null) {
                    K key = anchor.getKey();
                    metric = this.keyMetric.apply(key);
                }
                ByteIterable searchBi = BytesUtil.keyToBi(metric);
                cursor.getSearchKeyRange(searchBi);
                if (!anchor.isIncluding()) {
                    forwardWhile(() -> cursor.getKey().equals(searchBi), () -> {
                    });
                }
            }
        }

        @Override
        void gotoHighElement(ISecAnchor<K, M> anchor) {
            if (anchor == null) {
                cursor.getLast(); // Xodus bug
                cursor.getLast(); // Xodus bug
                cursor.getPrev();
                cursor.getNext();
            } else {
                M metric = anchor.getMetric();
                if (metric == null) {
                    K key = anchor.getKey();
                    M m = this.keyMetric.apply(key);
                    ByteIterable searchBi = BytesUtil.keyToBi(m);
                    cursor.getSearchKeyRange(searchBi);
                    if (!check()) {
                        cursor.getLast(); // Xodus bug
                        cursor.getLast(); // Xodus bug
                        cursor.getPrev();
                        cursor.getNext();
                        return;
                    }
                    forwardWhile(() -> indexValue().compareTo(m) == 0 && !get().equals(key), () -> {
                    });
                    if (!anchor.isIncluding()) {
                        cursor.getPrev();
                        if (!check() || get().equals(key)) {
                            cursorValid = false;
                        }
                    }
                } else {
                    ByteIterable searchBi = BytesUtil.keyToBi(metric);
                    cursor.getSearchKeyRange(searchBi);
                    if (cursor.getKey().getLength() == 0) {
                        cursor.getLast();
                        cursor.getLast();
                        cursor.getPrev(); // Xodus bug
                        cursor.getNext();
                    } else {
                        if (!anchor.isIncluding()) {
                            backwardWhile(() -> cursor.getKey().compareTo(searchBi) >= 0, () -> {
                            });
                        }
                    }
                }
            }
        }

    }

    private class SorterStoreProcessor<K extends Comparable<? super K>> extends AStoreProcessor<K, K> {

        SorterStoreProcessor(Class<K> keyClass) {
            super(keyClass, v -> v);
        }

        @Override
        K indexValue() {
            return get();
        }
    }

    private class StoreIndexProcessor<K, V, M extends Comparable<? super M>> extends AStoreProcessor<K, M> implements IStoreIndex<K, M> {

        private final Store store;
        private final Function<K, M> keyMetric;
        private final Function<V, M> valueMetric;
        private final IStore<K, V> parentStore;

        StoreIndexProcessor(Class<K> keyClass, Function<V, M> valueMetric, Function<K, M> keyMetric, String storeName, IStore<K, V> parentStore) {
            super(keyClass, keyMetric);
            this.keyMetric = keyMetric;
            this.valueMetric = valueMetric;
            this.parentStore = parentStore;
            this.store = env.openStore(storeName, StoreConfig.WITH_DUPLICATES, tx);
        }

        public void add(V newValue, K key) {
            ByteIterable keyBi = BytesUtil.keyToBi(key);
            ByteIterable metricBi = BytesUtil.keyToBi(valueMetric.apply(newValue));
            store.put(tx, metricBi, keyBi);
        }

        public void remove(V oldValue, K key) {
            ByteIterable keyBi = BytesUtil.keyToBi(key);
            ByteIterable metricBi = BytesUtil.keyToBi(valueMetric.apply(oldValue));
            try (Cursor cursor = store.openCursor(tx)) {
                boolean find = cursor.getSearchBoth(metricBi, keyBi);
                if (find) {
                    cursor.deleteCurrent();
                } else {
                    System.err.println("Warning: cannot find index " + key + " -> " + oldValue + "  actual value is " + keyMetric.apply(key));
                    // throw new VelvetException("Delete key not found " + metricBi);
                }
            }
        }

        @Override
        K get() {
            return BytesUtil.keyBiToObj(keyClass, cursor.getValue());
        }

        @Override
        M indexValue() {
            return keyMetric.apply(get());
        }

        @Override
        public List<K> keys(ISecQuery<K, M> query) {
            return go(store, query);
        }

        @Override
        public void recalculate() {
            try (Cursor cursor = store.openCursor(tx)) {
                while(cursor.deleteCurrent());
            }
            List<K> keys = parentStore.keys();
            for (K key : keys) {
                add(parentStore.get(key), key);
            }
        }
    }

    private <T> T toObj(Class<T> clazz, ByteIterable bi) {
        ISerializer ze = serializerSupplier.get();
        byte[] bytes = bi.getBytesUnsafe();
        int length = bi.getLength();
        InputStream inputStream = new ByteArrayInputStream(bytes, 0, length);
        return ze.deserialize(inputStream, clazz);
    }

//    private byte[] biToByteArray(ByteIterable bi) {
//        byte[] bytes = bi.getBytesUnsafe();
//        int length = bi.getLength();
//        return Arrays.copyOf(bytes, length);
//    }

    private <T> ByteIterable toBi(T obj, Class<T> clazz) {
        ISerializer ze = serializerSupplier.get();
        byte[] bytes = ze.serialize(obj, clazz);
        return new ArrayByteIterable(bytes);
    }

    @Override
    public <HK, CK> ISingleLink<HK, CK> singleLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        return new SingleLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK> IMultiLink<HK, CK> multiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        return new MultiLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK extends Comparable<? super CK>> IPriIndexLink<HK, CK> primaryKeyIndex(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        return new PriIndexMultiLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK, T, M extends Comparable<? super M>> ISecIndexLink<HK, CK, M> secondaryKeyIndex(Class<HK> hostKeyClass, String edgekind, Function<T, M> nodeMetric, Class<M> mclazz, Class<CK> keyClazz, IStore<CK, T> childStore) {
        return new SecIndexMultiLink<>(hostKeyClass, edgekind, nodeMetric, mclazz, keyClazz, childStore);
    }

    private class SingleLink<HK, CK> implements ISingleLink<HK, CK> {

        private Store connectMap;
        private Class<CK> childClass;
        private Class<HK> hostClass;

        public SingleLink(Class<HK> hostClass, Class<CK> childClass, String edgeKind) {
            this.connectMap = env.openStore("#s/" + edgeKind, StoreConfig.WITHOUT_DUPLICATES, tx);
            this.hostClass = hostClass;
            this.childClass = childClass;
        }

        @Override
        public void put(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostClass);
            connectMap.put(tx, key1Bi, toBi(key2, childClass));
        }

        @Override
        public void delete(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostClass);
            connectMap.delete(tx, key1Bi);
        }

        @Override
        public List<CK> keys(HK hk) {
            ByteIterable key1Bi = toBi(hk, hostClass);
            ByteIterable key2Bi = connectMap.get(tx, key1Bi);
            return (key2Bi == null) ? Collections.emptyList() : Arrays.asList(toObj(childClass, key2Bi));
        }

        @Override
        public boolean contains(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostClass);
            ByteIterable key2Bi = connectMap.get(tx, key1Bi);
            return key2Bi != null && key2Bi.equals(toBi(key2, childClass));
        }

    }

    private class MultiLink<HK, CK> implements IMultiLink<HK, CK> {

        private Store connectMap;
        private Class<CK> childKeyClass;
        private Class<HK> hostKeyClass;

        public MultiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgeKind) {
            this.connectMap = env.openStore("#m/" + edgeKind, StoreConfig.WITH_DUPLICATES, tx);
            this.hostKeyClass = hostKeyClass;
            this.childKeyClass = childKeyClass;
        }

        @Override
        public void put(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            connectMap.put(tx, key1Bi, toBi(key2, childKeyClass));
        }

        @Override
        public void delete(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            try (Cursor cursor = connectMap.openCursor(tx)) {
                if (cursor.getSearchBoth(key1Bi, toBi(key2, childKeyClass)))
                    cursor.deleteCurrent();
            }
        }

        @Override
        public List<CK> keys(HK hk) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            try (Cursor cursor = connectMap.openCursor(tx)) {
                cursor.getSearchKey(key1Bi);
                ByteIterable bi = cursor.getKey();
                List<CK> result = new ArrayList<>();
                while (key1Bi.equals(bi)) {
                    result.add(toObj(childKeyClass, cursor.getValue()));
                    if (!cursor.getNext())
                        break;
                    bi = cursor.getKey();
                }
                return result;
            }
        }

        @Override
        public boolean contains(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            try (Cursor cursor = connectMap.openCursor(tx)) {
                return cursor.getSearchBoth(key1Bi, toBi(key2, childKeyClass));
            }
        }
    }

    private abstract class ALinkProcessor<K, M extends Comparable<? super M>> extends ARangeQueryProcessor<K, M> {

        protected Class<K> keyClass;
        private Class<M> mClass;
        private ByteIterable key1Bi;
        private Function<K, M> keyMetric;

        ALinkProcessor(Class<K> keyClass, Class<M> mClass, ByteIterable key1Bi, Function<K, M> keyMetric) {
            this.keyClass = keyClass;
            this.mClass = mClass;
            this.key1Bi = key1Bi;
            this.keyMetric = keyMetric;
        }

        @Override
        M indexValue() {
            ByteIterable mbi = BytesUtil.extract2(cursor.getKey());
            return BytesUtil.keyBiToObj(mClass, mbi);
        }

        @Override
        boolean check() {
            ByteIterable key = cursor.getKey();
            return cursorValid && key.getLength() != 0 && BytesUtil.extract1(key).equals(key1Bi);
        }

        @Override
        void gotoLowElement(ISecAnchor<K, M> anchor) {
            if (anchor == null) {
                cursor.getSearchKeyRange(BytesUtil.join(key1Bi, ByteIterable.EMPTY));
            } else {
                M search = anchor.getMetric();
                if (search != null) {
                    ByteIterable searchBi = BytesUtil.join(key1Bi, BytesUtil.keyToBi(search));
                    cursor.getSearchKeyRange(searchBi);
                    if (!anchor.isIncluding()) {
                        forwardWhile(() -> indexValue().compareTo(search) == 0, () -> {
                        });
                    }
                } else {
                    K key = anchor.getKey();
                    M m = keyMetric.apply(key);
                    if (m == null) {
                        throw new VelvetException("null secondary key value returned for primary key " + key);
                    }
                    ByteIterable searchBi = BytesUtil.join(key1Bi, BytesUtil.keyToBi(m));
                    cursor.getSearchKeyRange(searchBi);
                    forwardWhile(() -> indexValue().compareTo(m) == 0 && !get().equals(key), () -> {
                    });
                    if (!anchor.isIncluding() && get().equals(key))
                        cursorValid = cursor.getNext();
                }
            }

        }

        @Override
        void gotoHighElement(ISecAnchor<K, M> anchor) {
            if (anchor == null) { // Warn: this is slow
                seekLast();
            } else {
                M search = anchor.getMetric();
                if (search != null) {
                    ByteIterable searchBi = BytesUtil.join(key1Bi, BytesUtil.keyToBi(search));
                    cursor.getSearchKeyRange(searchBi);
                    if (!check()) {
                        seekLast(); // TODO: can be optimized ? just step back
                        return;
                    }
                    if (anchor.isIncluding()) {
                        forwardWhile(() -> indexValue().compareTo(search) == 0, () -> {
                        });
                    }
                    oneStepBack();
                } else {
                    K key = anchor.getKey();
                    M m = keyMetric.apply(key);
                    if (m == null) {
                        throw new VelvetException("null value received for key " + key + " during secondary index calculation");
                    }
                    ByteIterable searchBi = BytesUtil.join(key1Bi, BytesUtil.keyToBi(m));
                    cursor.getSearchKeyRange(searchBi);
                    if (!check()) {
                        seekLast(); // TODO: can be optimized ? just step back
                        return;
                    }
                    forwardWhile(() -> indexValue().compareTo(m) == 0 && !get().equals(key), () -> {
                    });
                    if (!anchor.isIncluding()) {
                        oneStepBack();
//                        if (indexValue().compareTo(m) == 0) {
//                            cursorValid = false;
//                        }
                    }
                }
            }
        }

        private void oneStepBack() {
            if (!cursorValid) {
                cursorValid = true;
                cursor.getLast(); // Workaround for xodus bug
                cursor.getLast();
                cursor.getPrev();
                cursor.getNext();
            } else {
                cursorValid = cursor.getPrev();
            }

        }

        private void seekLast() {
            cursor.getSearchKeyRange(BytesUtil.join(key1Bi, ByteIterable.EMPTY));
            forwardWhile(() -> true, () -> {
            });
            oneStepBack();
        }
    }

    /**
     * key: [key1][key2], value: empty
     */
    private class PriIndexMultiLink<HK, CK extends Comparable<? super CK>> implements IPriIndexLink<HK, CK> {

        private Store connectMap;
        private Class<CK> childKeyClass;
        private Class<HK> hostKeyClass;

        public PriIndexMultiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgeKind) {
            this.connectMap = env.openStore("#mp/" + edgeKind, StoreConfig.WITHOUT_DUPLICATES, tx);
            this.hostKeyClass = hostKeyClass;
            this.childKeyClass = childKeyClass;
        }

        @Override
        public void put(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            ByteIterable compKey = BytesUtil.join(key1Bi, BytesUtil.keyToBi(key2));
            // System.err.println(compKey);
            connectMap.put(tx, compKey, ByteIterable.EMPTY);
        }

        @Override
        public void delete(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            connectMap.delete(tx, BytesUtil.join(key1Bi, BytesUtil.keyToBi(key2)));
        }

        @Override
        public List<CK> keys(HK hk) {
            return keys(hk, KeyQueries.<CK> builder().build());
        }

        @Override
        public boolean contains(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            ByteIterable compKey = BytesUtil.join(key1Bi, BytesUtil.keyToBi(key2));
            return connectMap.exists(tx, compKey, ByteIterable.EMPTY);
        }

        @Override
        public List<CK> keys(HK hk, IKeyQuery<CK> query) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            ISecQuery<CK, CK> mquery = SecQueries.from(query);
            return new PriLinkProcessor(childKeyClass, childKeyClass, key1Bi).go(connectMap, mquery);
        }

        private class PriLinkProcessor extends ALinkProcessor<CK, CK> {

            PriLinkProcessor(Class<CK> keyClass, Class<CK> mClass, ByteIterable key1Bi) {
                super(keyClass, mClass, key1Bi, x -> x);
            }

            @Override
            CK get() {
                ByteIterable k2bi = BytesUtil.extract2(cursor.getKey());
                CK key = BytesUtil.keyBiToObj(keyClass, k2bi);
                return key;
            }

        }

    }

    /**
     * key: [key1][weight], value: key2
     */
    private class SecIndexMultiLink<HK, CK, V, M extends Comparable<? super M>> implements ISecIndexLink<HK, CK, M> {

        private Store connectMap;
        private Function<CK, M> keyMetric;
        private Class<M> mclazz;
        private Class<CK> keyClass;
        private Class<HK> hostKeyClass;

        public SecIndexMultiLink(Class<HK> hostKeyClass, String edgeKind, Function<V, M> nodeMetric, Class<M> mclazz, Class<CK> keyClass, IStore<CK, V> childStore) {
            this.connectMap = env.openStore("#ms/" + edgeKind, StoreConfig.WITH_DUPLICATES, tx);
            this.hostKeyClass = hostKeyClass;
            this.mclazz = mclazz;
            this.keyClass = keyClass;
            this.keyMetric = key -> {
                V v = childStore.get(key);
                return v == null ? null : nodeMetric.apply(v);
            };
        }

        @Override
        public void put(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            ByteIterable compKey = BytesUtil.join(key1Bi, BytesUtil.keyToBi(keyMetric.apply(key2)));
            connectMap.put(tx, compKey, toBi(key2, keyClass));
        }

        @Override
        public void delete(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            M metric = keyMetric.apply(key2);
            if (metric == null)
                return;
            ByteIterable searchKey = BytesUtil.join(key1Bi, BytesUtil.keyToBi(metric));
            try (Cursor cursor = connectMap.openCursor(tx)) {
                boolean find = cursor.getSearchBoth(searchKey, toBi(key2, keyClass));
                if (find) {
                    cursor.deleteCurrent();
                } else {
                    throw new VelvetException("Delete key not found " + key2);
                }
            }
        }

        @Override
        public List<CK> keys(HK hk) {
            return keys(hk, SecQueries.<CK, M> builder().build());
        }

        @Override
        public boolean contains(HK hk, CK key2) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            M metric = keyMetric.apply(key2);
            if (metric == null)
                return false;
            try (Cursor cursor = connectMap.openCursor(tx)) {
                ByteIterable searchKey = BytesUtil.join(key1Bi, BytesUtil.keyToBi(metric));
                return cursor.getSearchBoth(searchKey, toBi(key2, keyClass));
            }
        }

        @Override
        public void update(HK hk, CK key2) {
            // TODO: it's a problem ! don't have old values
        }

        @Override
        public List<CK> keys(HK hk, ISecQuery<CK, M> query) {
            ByteIterable key1Bi = toBi(hk, hostKeyClass);
            return new SecLinkProcessor(keyClass, mclazz, key1Bi, keyMetric).go(connectMap, query);
        }

        private class SecLinkProcessor extends ALinkProcessor<CK, M> {

            SecLinkProcessor(Class<CK> keyClass, Class<M> mClass, ByteIterable key1Bi, Function<CK, M> keyMetric) {
                super(keyClass, mClass, key1Bi, keyMetric);
            }

            @Override
            CK get() {
                ByteIterable key2bi = cursor.getValue();
                return toObj(keyClass, key2bi);
            }

        }

    }

    private void debug(Store store) {
        /*
         * System.err.println("STORE-----------------------------"); try (Cursor c = store.openCursor(tx)) { while (c.getNext()) { System.err.println(c.getKey() + " --> " + c.getValue()); } }
         */
    }

}
