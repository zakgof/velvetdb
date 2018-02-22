package com.zakgof.db.velvet.dynamodb;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.*;
import com.zakgof.serialize.ISerializer;
import com.zakgof.tools.generic.Pair;

public class DynamoDBVelvet implements IVelvet {

    private Supplier<ISerializer> serializerSupplier;
    private DynamoDB db;

    private ScalarAttributeType keyType(Class<?> cl) {
        if (cl.equals(String.class))
            return ScalarAttributeType.S;
        else if (Number.class.isAssignableFrom(Primitives.wrap(cl)))
            return ScalarAttributeType.N;
        throw new VelvetException("Unsupported key class for dynamodb velvet: " + cl);
    }

    private static <T> T calcOnTable(Callable<T> callable, Runnable tableCreator) {
        try {
            try {
                return callable.call();
            } catch (ResourceNotFoundException e) {
                try {
                    tableCreator.run();
                    return callable.call();
                } catch (ResourceNotFoundException e2) {
                    System.err.println("ERROR : Table not found again: " + e2);
                    return callable.call();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new VelvetException(e);
        }
    }

    private void doOnTable(Runnable runnable, Runnable tableCreator) {
        try {
            runnable.run();
        } catch (ResourceNotFoundException e) {
            try {
                tableCreator.run();
                runnable.run();
            } catch (ResourceNotFoundException e2) {
                System.err.println("ERROR : Table not found again: " + e2);
                try {
                    Thread.sleep(8000);
                } catch (InterruptedException e1) {
                }
                runnable.run();
            }
        }
    }

    private <V> V valueFromItem(Item item, Class<V> cl, String attrName, boolean isKey) {
        cl = Primitives.wrap(cl);
        if (isKey && cl.equals(String.class)) {
            return cl.cast(item.getString(attrName));
        } else if (cl.equals(Long.class)) {
            return cl.cast(item.getLong(attrName));
        } else if (cl.equals(Integer.class)) {
            return cl.cast(item.getInt(attrName));
        } else if (cl.equals(Short.class)) {
            return cl.cast(item.getShort(attrName));
        } else if (cl.equals(Byte.class)) {
            return cl.cast((byte)item.getShort(attrName));
        }
        // TODO
        byte[] bytes = item.getBinary("v");
        V value = serializerSupplier.get().deserialize(new ByteArrayInputStream(bytes), cl);
        return value;
    }

    private Table makeTable(CreateTableRequest request) {
        Table table = db.createTable(request);
        try {
            System.err.print("Creating table " + request.getTableName() + "... ");
            table.waitForActive();
            System.err.println("done");
        } catch (InterruptedException e) {
            throw new VelvetException(e);
        }
        return table;
    }

    private CreateTableRequest makeTableRequest(String kind, KeySchemaElement[] keySchemaElements, AttributeDefinition[] attributeDefinitions) {
        return new CreateTableRequest()
           .withTableName(kind)
           .withKeySchema(keySchemaElements)
           .withAttributeDefinitions(attributeDefinitions)
           .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    }

    private CreateTableRequest makeTableRequest(String kind, String hashKeyName, ScalarAttributeType hashKeyType) {
        return makeTableRequest(kind,
            new KeySchemaElement[] {new KeySchemaElement(hashKeyName, KeyType.HASH)},
            new AttributeDefinition[] {new AttributeDefinition(hashKeyName, hashKeyType)}
        );
    }

    private CreateTableRequest makeTableRequest(String kind, String hashKeyName, ScalarAttributeType hashKeyType, String rangeKeyName, ScalarAttributeType rangeKeyType) {
        return makeTableRequest(kind,
            new KeySchemaElement[] {new KeySchemaElement(hashKeyName, KeyType.HASH), new KeySchemaElement(rangeKeyName, KeyType.RANGE)},
            new AttributeDefinition[] {new AttributeDefinition(hashKeyName, hashKeyType), new AttributeDefinition(rangeKeyName, rangeKeyType)}
        );
    }

    private <CK, M extends Comparable <? super M>, V> boolean filterM(Item item, ISecQuery<CK, M> query, M lowM2, M highM2, Class<CK> keyClass, Class<M> mClass, String keyAttr, String mAttr) {
        CK ck = valueFromItem(item, keyClass, keyAttr, true);
        M m = valueFromItem(item, mClass, mAttr, true);
        ISecAnchor<CK, M> lowAnchor = query.getLowAnchor();
        if (lowAnchor != null) {
            int c = m.compareTo(lowM2);
            if (c < 0 || c == 0 && !lowAnchor.isIncluding() && lowAnchor.getKey() == null)
                return false;
            if (c == 0 && lowAnchor.getKey() != null && !lowAnchor.isIncluding() && ((Comparable<? super CK>)lowAnchor.getKey()).compareTo(ck) >= 0) {
                return false;
            }
        }
        ISecAnchor<CK, M> highAnchor = query.getHighAnchor();
        if (highAnchor != null) {
            int c = m.compareTo(highM2);
            if (c > 0 || c == 0 && !highAnchor.isIncluding() && highAnchor.getKey() == null)
                return false;
            if (c == 0 && highAnchor.getKey() != null && !highAnchor.isIncluding() && ((Comparable<? super CK>)highAnchor.getKey()).compareTo(ck) <= 0) {
                return false;
            }
        }
        return true;
    }

    private static <K, V, M extends Comparable<? super M>> M scanKeyMetric(IStore<K, V> store, Function<V, M> metric, K key) {
        V v = store.get(key);
        if (v == null) {
            throw new VelvetException("Attempting to get index from node with key " + key + " that does not exist.");
        }
        M m = metric.apply(v);
        return m;
    }

    private <K, V, M extends Comparable<? super M>> List<K> scanSecondary(ISecQuery<K, M> query, Index index, IStore<K, V> store, Function<V, M> metric, KeyAttribute hashKey, Class<K> keyClass, Class<M> mClass, String keyAttr, String mAttr, Runnable tableCreator) {

            QuerySpec qs = new QuerySpec()
                    .withAttributesToGet(keyAttr, mAttr)
                    .withHashKey(hashKey);

            ISecAnchor<K, M> lowAnchor = query.getLowAnchor();
            ISecAnchor<K, M> highAnchor = query.getHighAnchor();
            M lowM = null;
            M highM = null;
            if (lowAnchor != null) {
                lowM = lowAnchor.getKey() == null ? lowAnchor.getMetric() : scanKeyMetric(store, metric, lowAnchor.getKey());
            }
            if (highAnchor != null) {
                highM = highAnchor.getKey() == null ? highAnchor.getMetric() : scanKeyMetric(store, metric, highAnchor.getKey());
            }
            if (lowM != null && highM == null) {
                RangeKeyCondition condition = new RangeKeyCondition(mAttr);
                condition = lowAnchor.isIncluding() || lowAnchor.getKey() != null ? condition.ge(lowM) : condition.gt(lowM);
                qs.withRangeKeyCondition(condition);
            } else if (lowM == null && highM != null) {
                RangeKeyCondition condition = new RangeKeyCondition(mAttr);
                condition = highAnchor.isIncluding() || highAnchor.getKey() != null ? condition.le(highM) : condition.lt(highM);
                qs.withRangeKeyCondition(condition);
            } else if (lowM != null && highM != null) {
                if (lowM.compareTo(highM) > 0)
                    return Collections.emptyList();
                RangeKeyCondition condition = new RangeKeyCondition(mAttr).between(lowM, highM);
                qs.withRangeKeyCondition(condition);
            }
    //        TODO: perf: is any limit possible ?
    //        if (query.getLimit() > 0) {
    //            int number = query.getLimit() + query.getOffset();
    //            qs.withMaxResultSize(number + 2); // possible exclusion
    //        }
            if (!query.isAscending()) {
                qs.withScanIndexForward(false);
            }
            M lowM2 = lowM;
            M highM2 = highM;
            List<K> cks = calcOnTable(() -> {
                ItemCollection<QueryOutcome> itemCollection = index.query(qs);

                List<Item> debug1 = StreamSupport.stream(itemCollection.spliterator(), false)
                        .collect(Collectors.toList());

                List<Item> debug2 = StreamSupport.stream(itemCollection.spliterator(), false)
                        .filter(item -> filterM(item, query, lowM2, highM2, keyClass, mClass, keyAttr, mAttr))
                        .sorted(itemComparator(keyClass, keyAttr, mClass, mAttr, query.isAscending()))
                        .collect(Collectors.toList());

                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .filter(item -> filterM(item, query, lowM2, highM2, keyClass, mClass, keyAttr, mAttr))
                    // TODO: have to sort by pri key if metrics equals
                    .sorted(itemComparator(keyClass, keyAttr, mClass, mAttr, query.isAscending()))
                    .skip(query.getOffset())
                    .limit(query.getLimit() >= 0 ? query.getLimit() : Integer.MAX_VALUE)
                    .map(item -> valueFromItem(item, keyClass, keyAttr, true))
                    .collect(Collectors.toList());
            }, tableCreator);
            return cks;
        }

    private <K, M extends Comparable<? super M>> Comparator<? super Item> itemComparator(Class<K> keyClass, String keyAttr, Class<M> mClass, String mAttr, boolean isAscending) {
        Comparator<Item> comparator = Comparator.<Item, M>comparing(item -> valueFromItem(item, mClass, mAttr, true)).thenComparing(item -> (Comparable)valueFromItem(item, keyClass, keyAttr, true));
        if (!isAscending) {
            comparator = comparator.reversed();
        }
        return comparator;
    }

    private void cleanTable(Table table) {
        System.err.print("Cleaning up " + table.getTableName() + "... ");
        try {

            if (table.getDescription() == null)
                table.describe();
            List<KeySchemaElement> keySchema = table.getDescription().getKeySchema();
            String[] attrs = keySchema.stream().map(KeySchemaElement::getAttributeName).toArray(String[]::new);
            ItemCollection<ScanOutcome> itemCollection = table.scan(new ScanSpec().withAttributesToGet(attrs));
            List<PrimaryKey> pks = StreamSupport.stream(itemCollection.spliterator(), false)
                .map((Item item) -> itemToPrimaryKey(item))
                .collect(Collectors.toList());
            System.err.println(" " + pks.size() + " items"  );


            Iterator<PrimaryKey> iterator = pks.iterator();
            List<WriteRequest> unprocessed = null;
            for (;;) {
                BatchWriteItemSpec bwis = new BatchWriteItemSpec();
                TableWriteItems tableWriteItems = new TableWriteItems(table.getTableName());
                if (unprocessed != null && !unprocessed.isEmpty()) {
                    bwis.withUnprocessedItems(ImmutableMap.of(table.getTableName(), unprocessed));
                } else if (iterator.hasNext()) {
                    for (int i = 0; i < 25 && iterator.hasNext(); i++) {
                        tableWriteItems.addPrimaryKeyToDelete(iterator.next());
                    }
                    bwis.withTableWriteItems(tableWriteItems);
                }
                if (bwis.getUnprocessedItems() == null && bwis.getTableWriteItems() == null) {
                    break;
                }
                System.err.print("   deleting a batch... ");
                BatchWriteItemOutcome outcome = db.batchWriteItem(bwis);
                unprocessed = outcome.getUnprocessedItems().get(table.getTableName());
                if (unprocessed != null && !unprocessed.isEmpty()) {
                    System.err.println("   " + unprocessed.size() + " inprocessed items");
                } else {
                    System.err.println("   done");
                }
            }
            System.err.println("Deleting complete");
        } catch (ResourceNotFoundException e) {
            System.err.println("error " + e);
        }
    }

    private PrimaryKey itemToPrimaryKey(Item item) {
        KeyAttribute[] keyAttributes = item.asMap().entrySet().stream().map(e -> new KeyAttribute(e.getKey(), e.getValue())).toArray(KeyAttribute[]::new);
        return new PrimaryKey(keyAttributes);
    }

    private <K, V> Map<K, V> batchGet(List<K> keys, String kind, Function<K, KeyAttribute[]> keyFunc, List<String> attrs, Function<Item, K> kGetter, Function<Item, V> vGetter, Runnable tableCreator) {
        keys = keys.stream().distinct().collect(Collectors.toList());
        Iterator<K> kiterator = keys.iterator();
        Map<K, V> result = new HashMap<>();
        Map<String, KeysAndAttributes> unprocessedKeys = new HashMap<>();
        while (kiterator.hasNext()) {
            TableKeysAndAttributes tableKeyAndAttributes = new TableKeysAndAttributes(kind).withAttributeNames(attrs);
            // TODO: workaround for an AWS SDK bug:
            if (unprocessedKeys.isEmpty()) {
                for (int i = unprocessedKeys.size(); i < 25 && kiterator.hasNext(); i++) {
                    K key = kiterator.next();
                    tableKeyAndAttributes.addPrimaryKey(new PrimaryKey().addComponents(keyFunc.apply(key)));
                }
            }
            BatchGetItemSpec bgis = new BatchGetItemSpec().withTableKeyAndAttributes(tableKeyAndAttributes);
            if (!unprocessedKeys.isEmpty())
                bgis.withUnprocessedKeys(unprocessedKeys);
            System.err.print("  reading a chunk: " + tableKeyAndAttributes.getPrimaryKeys().size() + " items + " + unprocessedKeys.size() + " unprocessed items...");
            BatchGetItemOutcome outcome = calcOnTable(() -> db.batchGetItem(bgis), tableCreator);
            Map<String, KeysAndAttributes> newUnprocessedItems = outcome.getUnprocessedKeys();
            System.err.println("   done with " + newUnprocessedItems.size() + " unprocessed");
            unprocessedKeys.putAll(newUnprocessedItems);
            Map<K, V> partialResult = outcome.getTableItems().get(kind).stream().collect(Collectors.toMap(kGetter, vGetter));
            result.putAll(partialResult);
        }
        LinkedHashMap<K, V> map = keys.stream().distinct().map(key -> Pair.create(key, result.get(key))).filter(p -> p.second() != null).collect(Collectors.toMap(Pair::first, Pair::second, (u, v) -> u, LinkedHashMap::new));
        return map;
    }

    DynamoDBVelvet(DynamoDB db, Supplier<ISerializer> serializerSupplier) {
        this.db = db;
        this.serializerSupplier = serializerSupplier;
    }

    @Override
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new Store<>(kind, keyClass, valueClass, indexes, null);
    }

    @Override
    public <K, V> IStore<K, V> storeWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes, IStore<K, V> parentStore) {
        return new Store<>(kind, keyClass, valueClass, indexes, parentStore);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedStore<>(kind, keyClass, valueClass, indexes, null);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStoreWithProxy(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes, IStore<K, V> parentStore) {
        return new SortedStore<>(kind, keyClass, valueClass, indexes, parentStore);
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
        return new PrimaryMultiLink<>(hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK, CV, M extends Comparable<? super M>> ISecIndexLink<HK, CK, M> secondaryKeyIndex( Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> mclazz, Class<CK> childKeyClazz, IStore<CK, CV> childStore) {
        return new SecondaryMultiLink<>(hostKeyClass, edgekind, nodeMetric, mclazz, childKeyClazz, childStore);
    }

    public void killAll(boolean full) {
        TableCollection<ListTablesResult> listTables = db.listTables();

        for (Table table : listTables) {
            if (full) {
                System.err.print("Deleting " + table.getTableName() + "... ");
                table.delete();
                try {
                    table.waitForDelete();
                    System.err.println("done.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                cleanTable(table);
            }
        }
        System.err.println("----------------");
    }

    private class Store<K, V> implements IStore<K, V> {

        protected Class<K> keyClass;
        protected String kind;
        protected Class<V> valueClass;
        protected Table table;
        protected Collection<IStoreIndexDef<?, V>> indexes;
        private Map<String, StoreIndex<?>> indexMap;
        private IStore<K, V> parentStore;

        /**
         * hash key: id
         * attribute: v
         *
         * indexes:
         * common hash key: partition
         * range key: index-indexname
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public Store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes, IStore<K, V> parentStore) {
            this.parentStore =  parentStore == null ? this:  parentStore;
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.kind = kind;
            this.table = db.getTable(kind);
            this.indexes = indexes;
            this.indexMap = indexes.stream().collect(Collectors.toMap(IStoreIndexDef::name, index -> new StoreIndex(index)));
        }

        @Override
        public V get(K key) {
            System.err.println("DynamoDB get " + kind + "/" + key);
            Item item = calcOnTable(() -> getByKey(key), this::createTable);
            if (item == null) {
                return null;
            }
            V value = valueFromItem(item, valueClass, "v", false);
            return value;
        }

        @Override
        public Map<K, V> batchGet(List<K> keys) {
            System.err.println("DynamoDB batch get " + kind + "/" + keys);
            return DynamoDBVelvet.this.batchGet(keys, kind, this::keyFor, Arrays.asList("id", "v"),
                                                item -> valueFromItem(item, keyClass, "id", true),
                                                item -> valueFromItem(item, valueClass, "v", false),  this::createTable);
        }

        @Override
        public byte[] getRaw(K key) {
            return null;
        }

        @Override
        public void put(K key, V value) {
            Object v = valueToObject(value);
            Item item = createPutItem(key, v, value);
            doOnTable(() -> table.putItem(item), this::createTable);
        }

        @Override
        public void put(List<K> keys, List<V> values) {
            Iterator<V> iterator = values.iterator();
            Iterator<K> kiterator = keys.iterator();
            List<WriteRequest> unprocessedItems = new ArrayList<>();
            System.err.println("Writing a batch of " + keys.size());
            int current = 0;
            long backoff = 100L;
            while (iterator.hasNext()) {
                BatchWriteItemSpec bwis = new BatchWriteItemSpec();
                // TODO: workaround for an AWS SDK bug:
                if (unprocessedItems.isEmpty()) {
                    TableWriteItems twi = new TableWriteItems(kind);
                    List<Item> items = new ArrayList<>(25);
                    for (int i = unprocessedItems.size(); i < 25 && iterator.hasNext(); i++) {
                        V value = iterator.next();
                        Object v = valueToObject(value);
                        Item item = createPutItem(kiterator.next(), v, value);
                        current++;
                        items.add(item);
                    }
                    twi.withItemsToPut(items);
                    bwis.withTableWriteItems(twi);
                }

                if (!unprocessedItems.isEmpty())
                    bwis.withUnprocessedItems(ImmutableMap.of(kind, unprocessedItems));
                System.err.print("Writing a batch: " + (bwis.getTableWriteItems() == null ? 0 : bwis.getTableWriteItems().iterator().next().getItemsToPut().size()) + " items + " + unprocessedItems.size() + " unprocessed items...");
                BatchWriteItemOutcome outcome = calcOnTable(() -> db.batchWriteItem(bwis), this::createTable);
                List<WriteRequest> newUnprocessedItems = outcome.getUnprocessedItems().get(kind);
                if (newUnprocessedItems == null)
                    newUnprocessedItems = new ArrayList<>();
                System.err.println("   ...done with " + newUnprocessedItems.size() + " unprocessed, still pending = " + (keys.size() - current));
                if (newUnprocessedItems.isEmpty()) {
                    backoff = 100L;
                } else {
                    try {
                        System.err.println("Backing off " + backoff);
                        Thread.sleep(backoff);
                    } catch (InterruptedException e) {
                    }
                    backoff = backoff << 1;
                }
                unprocessedItems = newUnprocessedItems;
            }
        }

        private Object valueToObject(V value) {
            if (Number.class.isAssignableFrom(Primitives.wrap(valueClass))) {
                return value;
            }
            byte[] bytes = serializerSupplier.get().serialize(value, valueClass);
            return bytes;
        }

        @Override
        public K put(V value) {
            if (!keyClass.equals(Long.class)) {
                throw new VelvetException("Autogenerated key must be Long");
            }
            // TODO: weak
            long id = UUID.randomUUID().getLeastSignificantBits();
            K key = keyClass.cast(id);

            put(key, value);
            return key;
        }

        @Override
        public void delete(K key) {
            table.deleteItem(keyFor(key));
        }

        @Override
        public List<K> keys() {
            System.err.println("DynamoDB Store scan: " + kind);
            ItemCollection<ScanOutcome> itemCollection = calcOnTable(() -> table.scan(new ScanSpec().withAttributesToGet("id")), this::createTable);
            List<K> keys = StreamSupport.stream(itemCollection.spliterator(), false).map(this::keyFromItem).collect(Collectors.toList());
            return keys;
        }

        @Override
        public boolean contains(K key) {
            Item item = calcOnTable(() -> table.getItem(keyFor(key)), this::createTable);
            return item != null;
        }

        @Override
        public long size() {
            // TODO
            return keys().size();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            IStoreIndex<K, M> index = (IStoreIndex<K, M>) indexMap.get(name);
            if (index == null) {
                throw new VelvetException("Index not found " + name);
            }
            return index;
        }

        protected Item getByKey(K key) {
            return table.getItem("id", key);
        }

        protected void createTable() {
            CreateTableRequest tableRequest = makeTableRequest(kind, "id",  keyType(keyClass));
            if (!indexes.isEmpty()) {
                tableRequest.withAttributeDefinitions(new AttributeDefinition("partition", ScalarAttributeType.N));
                for (IStoreIndexDef<?, V> index : indexes) {
                    GlobalSecondaryIndex gsi = new GlobalSecondaryIndex()
                        .withIndexName("index-" + index.name())
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
                        .withKeySchema(new KeySchemaElement("partition", KeyType.HASH),
                                       new KeySchemaElement("i-" + index.name(), KeyType.RANGE));

                    tableRequest.withAttributeDefinitions(new AttributeDefinition("i-" + index.name(), keyType(index.clazz())))
                        .withGlobalSecondaryIndexes(gsi);
                }
            }
            table = makeTable(tableRequest);
        }

        protected Item createPutItem(K key, Object v, V value) {
            Item item = new Item().with("v", v).withKeyComponents(keyFor(key));
            if (!indexes.isEmpty()) {
                item.with("partition", 1);
                for (IStoreIndexDef<?, V> index: indexes) {
                    item.with("i-" + index.name(), index.metric().apply(value));
                }
            }
            return item;
        }

        protected KeyAttribute[] keyFor(K key) {
            return new KeyAttribute[] {new KeyAttribute("id", key)};
        }

        protected K keyFromItem(Item item) {
            Class<K> kl = Primitives.wrap(keyClass);
            if (keyClass.equals(String.class)) {
                return kl.cast(item.get("id"));
            } else if (kl.equals(Long.class)) {
                return kl.cast(item.getLong("id"));
            } else if (kl.equals(Integer.class)) {
                return kl.cast(item.getInt("id"));
            } else if (kl.equals(Short.class)) {
                return kl.cast(item.getShort("id"));
            } else if (kl.equals(Byte.class)) {
                return kl.cast((byte) item.getShort("id"));
            }
            throw new VelvetException("Unsupported key class for dynamodb velvet: " + keyClass);
        }

        private class StoreIndex<M extends Comparable<? super M>> implements IStoreIndex<K, M> {

            private IStoreIndexDef<M, V> indexDef;

            private StoreIndex(IStoreIndexDef<M, V> indexDef) {
                this.indexDef = indexDef;
            }

            @Override
            public List<K> keys(ISecQuery<K, M> query) {
                System.err.println("DynamoDB store index query: " + kind + "/" + indexDef.name() + " " + query);
                Index index = table.getIndex("index-" + indexDef.name());
                KeyAttribute hashKey = new KeyAttribute("partition", 1);
                return scanSecondary(query, index, parentStore, indexDef.metric(), hashKey, keyClass, indexDef.clazz(), "id", "i-" + indexDef.name(), Store.this::createTable);
            }
        }
    }

    /**
     * hash key: partition = 1
     * range key: id = key
     * attribute: v = value
     *
     * indexes:
     * LSI
     * range key: index-indexname
     */
    private class SortedStore <K extends Comparable<? super K>, V> extends Store<K, V> implements ISortedStore<K, V> {

        public SortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes, IStore<K, V> parentStore) {
            super(kind, keyClass, valueClass, indexes, parentStore);
        }

        @Override
        public List<K> keys(IKeyQuery<K> query) {

            System.err.println("DynamoDB SortedStore query: " + kind);

            QuerySpec qs = new QuerySpec()
                .withAttributesToGet("id")
                .withHashKey("partition", 1);

            IKeyAnchor<K> lowAnchor = query.getLowAnchor();
            IKeyAnchor<K> highAnchor = query.getHighAnchor();
            K lowKey = null;
            K highKey = null;
            Predicate<K> fixBeetween = (ck) -> true;
            if (lowAnchor != null) {
                lowKey = lowAnchor.getKey();
            }
            if (highAnchor != null) {
                highKey = highAnchor.getKey();
            }
            if (lowKey != null && highKey == null) {
                RangeKeyCondition condition = new RangeKeyCondition("id");
                condition = lowAnchor.isIncluding() ? condition.ge(lowKey) : condition.gt(lowKey);
                qs.withRangeKeyCondition(condition);
            } else if (lowKey == null && highKey != null) {
                RangeKeyCondition condition = new RangeKeyCondition("id");
                condition = highAnchor.isIncluding() ? condition.le(highKey) : condition.lt(highKey);
                qs.withRangeKeyCondition(condition);
            } else if (lowKey != null && highKey != null) {
                if (lowKey.compareTo(highKey) > 0)
                    return Collections.emptyList();
                RangeKeyCondition condition = new RangeKeyCondition("id").between(lowKey, highKey);
                qs.withRangeKeyCondition(condition);
                K lowKey2 = lowKey;
                K highKey2 = highKey;
                fixBeetween = (ck) -> (lowAnchor.isIncluding() || !ck.equals(lowKey2)) && (highAnchor.isIncluding() || !ck.equals(highKey2));
            }
            if (query.getLimit() > 0) {
                int number = query.getLimit() + query.getOffset();
                qs.withMaxResultSize(number + 2); // possible exclusion
            }
            if (!query.isAscending()) {
                qs.withScanIndexForward(false);
            }
            Predicate<K> fixBeetween2 = fixBeetween;
            List<K> keys = calcOnTable(() ->  {
                ItemCollection<QueryOutcome> itemCollection = table.query(qs);
                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .map(this::keyFromItem)
                    .filter(fixBeetween2)
                    .skip(query.getOffset())
                    .limit(query.getLimit() >= 0 ? query.getLimit() : Integer.MAX_VALUE)
                    .collect(Collectors.toList());

            }, this::createTable);
            return keys;
        }

        @Override
        protected void createTable() {
            CreateTableRequest tableRequest = makeTableRequest(kind, "partition", ScalarAttributeType.N, "id", keyType(keyClass));
            for (IStoreIndexDef<?, V> index : indexes) {
                LocalSecondaryIndex lsi = new LocalSecondaryIndex()
                    .withIndexName("index-" + index.name())
                    .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
                    .withKeySchema(new KeySchemaElement("partition", KeyType.HASH),
                                   new KeySchemaElement("i-" + index.name(), KeyType.RANGE));

                tableRequest.withAttributeDefinitions(new AttributeDefinition("i-" + index.name(), keyType(index.clazz())))
                    .withLocalSecondaryIndexes(lsi);
            }
            table = makeTable(tableRequest);
        }


        @Override
        protected Item getByKey(K key) {
            return table.getItem(new GetItemSpec().withPrimaryKey(keyFor(key)));
        }

        @Override
        protected KeyAttribute[] keyFor(K key) {
            return new KeyAttribute[] {new KeyAttribute("partition", 1), new KeyAttribute("id", key)};
        }

        @Override
        protected Item createPutItem(K key, Object v, V value) {
            Item item = new Item()
                .with("v", v)
                .withKeyComponent("partition", 1)
                .withKeyComponent("id", key);
            for (IStoreIndexDef<?, V> index : indexes) {
                item.with("i-" + index.name(), index.metric().apply(value));
            }
            return item;
        }
    }

    /**
     * Hash key: hk
     * Value: ck
     */
    private class SingleLink<HK, CK> implements ISingleLink<HK, CK> {

        protected Table table;
        protected String tableName;
        protected Class<HK> hostKeyClass;
        protected Class<CK> childKeyClass;

        public SingleLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            this.tableName = prefix() + edgekind;
            this.hostKeyClass = hostKeyClass;
            this.childKeyClass = childKeyClass;
            this.table = db.getTable(tableName);
        }

        protected String prefix() {
            return "__s-";
        }

        @Override
        public void put(HK hk, CK ck) {
            Item item = createPutItem(hk, ck);
            doOnTable(() -> table.putItem(item), this::createTable);
        }

        @Override
        public void delete(HK hk, CK ck) {
            doOnTable(() -> table.deleteItem(keyFor(hk)), this::createTable);
        }

        @Override
        public List<CK> keys(HK hk) {
            System.err.println("DynamoDB SingleLink get " + tableName + "/" + hk);
            Item item = calcOnTable(() -> table.getItem(new KeyAttribute("hk", hk)), this::createTable);
            if (item == null) {
                return Collections.emptyList();
            }
            CK ck = valueFromItem(item, childKeyClass, "ck", true);
            return Arrays.asList(ck);
        }

        @Override
        public Map<HK, CK> batchGet(List<HK> hks) {
            return DynamoDBVelvet.this.<HK, CK>batchGet(hks, tableName, k -> new KeyAttribute[]{keyFor(k)}, Arrays.asList("hk", "ck"),
                item -> valueFromItem(item, hostKeyClass, "hk", true),
                item -> valueFromItem(item, childKeyClass, "ck", true),  this::createTable);
        }

        @Override
        public boolean contains(HK hk, CK ck) {
            Item item = calcOnTable(() -> table.getItem(keyFor(hk)), this::createTable);
            if (item != null) {
                CK actual = valueFromItem(item, childKeyClass, "ck", true);
                return actual.equals(ck);
            }
            return false;
        }

        protected Item createPutItem(HK hk, CK ck) {
            return new Item()
               .withKeyComponents(keyFor(hk))
               .with("ck", ck);
        }

        protected void createTable() {
            table = makeTable(makeTableRequest(tableName, "hk", keyType(hostKeyClass)));
        }

        protected KeyAttribute keyFor(HK hk) {
            return new KeyAttribute("hk", hk);
        }

    }

    /**
     * MultiLink = PrimaryKeyLink
     * Hash key: hk
     * Range key: ck
     */
    private class MultiLink<HK, CK> extends SingleLink<HK, CK> implements IMultiLink<HK, CK> {

        public MultiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            super(hostKeyClass, childKeyClass, edgekind);
        }

        @Override
        protected String prefix() {
            return "__m-";
        }

        @Override
        public void delete(HK hk, CK ck) {
            doOnTable(() -> table.deleteItem(keyFor(hk, ck)), this::createTable);
        }

        @Override
        public List<CK> keys(HK hk) {
            System.err.println("DynamoDB MultiLink query " + tableName + "/" + hk);
            List<CK> cks = calcOnTable(() -> {
                ItemCollection<QueryOutcome> itemCollection = table.query(keyFor(hk));
                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .map(item -> valueFromItem(item, childKeyClass, "ck", true))
                    .collect(Collectors.toList());
            }, this::createTable);
            return cks;
        }

        @Override
        public boolean contains(HK hk, CK ck) {
            return calcOnTable(() -> table.getItem(keyFor(hk, ck)) != null, this::createTable);
        }

        @Override
        protected Item createPutItem(HK hk, CK ck) {
            return new Item()
               .withKeyComponents(keyFor(hk, ck));
        }

        @Override
        protected void createTable() {
            table = makeTable(makeTableRequest(tableName, "hk", keyType(hostKeyClass), "ck", keyType(childKeyClass)));
        }

        private KeyAttribute[] keyFor(HK hk, CK ck) {
            return new KeyAttribute[] {new KeyAttribute("hk", hk), new KeyAttribute("ck", ck)};
        }
    }

    private class PrimaryMultiLink<HK, CK extends Comparable<? super CK>> extends MultiLink<HK, CK> implements IPriIndexLink<HK, CK> {

        public PrimaryMultiLink(Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            super(hostKeyClass, childKeyClass, edgekind);
        }

        @Override
        public List<CK> keys(HK hk, IKeyQuery<CK> query) {

            System.err.println("DynamoDB PriMultiLink range query: " + tableName + "/" + hk);

            QuerySpec qs = new QuerySpec()
                    .withAttributesToGet("ck")
                    .withHashKey(keyFor(hk));


            IKeyAnchor<CK> lowAnchor = query.getLowAnchor();
            IKeyAnchor<CK> highAnchor = query.getHighAnchor();
            CK lowKey = null;
            CK highKey = null;
            Predicate<CK> fixBeetween = (ck) -> true;
            if (lowAnchor != null) {
                lowKey = lowAnchor.getKey();
            }
            if (highAnchor != null) {
                highKey = highAnchor.getKey();
            }
            if (lowKey != null && highKey == null) {
                RangeKeyCondition condition = new RangeKeyCondition("ck");
                condition = lowAnchor.isIncluding() ? condition.ge(lowKey) : condition.gt(lowKey);
                qs.withRangeKeyCondition(condition);
            } else if (lowKey == null && highKey != null) {
                RangeKeyCondition condition = new RangeKeyCondition("ck");
                condition = highAnchor.isIncluding() ? condition.le(highKey) : condition.lt(highKey);
                qs.withRangeKeyCondition(condition);
            } else if (lowKey != null && highKey != null) {
                if (lowKey.compareTo(highKey) > 0)
                    return Collections.emptyList();
                RangeKeyCondition condition = new RangeKeyCondition("ck").between(lowKey, highKey);
                qs.withRangeKeyCondition(condition);
                CK lowKey2 = lowKey;
                CK highKey2 = highKey;
                fixBeetween = (ck) -> (lowAnchor.isIncluding() || !ck.equals(lowKey2)) && (highAnchor.isIncluding() || !ck.equals(highKey2));
            }
            if (query.getLimit() > 0) {
                int number = query.getLimit() + query.getOffset();
                qs.withMaxResultSize(number + 2); // possible exclusion
            }
            if (!query.isAscending()) {
                qs.withScanIndexForward(false);
            }
            Predicate<CK> fixBeetween2 = fixBeetween;
            List<CK> cks = calcOnTable(() -> {
                ItemCollection<QueryOutcome> itemCollection = table.query(qs);

                // List<Item> debug = StreamSupport.stream(itemCollection.spliterator(), false).collect(Collectors.toList());

                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .map(item -> valueFromItem(item, childKeyClass, "ck", true))
                    .filter(fixBeetween2)
                    .skip(query.getOffset())
                    .limit(query.getLimit() >= 0 ? query.getLimit() : Integer.MAX_VALUE)
                    .collect(Collectors.toList());
            }, this::createTable);
            return cks;
        }

    }

    /**
     * Hash key : hk
     * Sort key : ck
     * LSI      :  m
     */
    private class SecondaryMultiLink<HK, CK, CV, M extends Comparable<? super M>> extends MultiLink<HK, CK> implements ISecIndexLink<HK, CK, M> {

        private Function<CV, M> nodeMetric;
        private IStore<CK, CV> childStore;
        private Class<M> mclazz;

        public SecondaryMultiLink(Class<HK> hostKeyClass, String edgekind, Function<CV, M> nodeMetric, Class<M> mclazz, Class<CK> childKeyClass, IStore<CK, CV> childStore) {
            super(hostKeyClass, childKeyClass, edgekind);
            this.nodeMetric = nodeMetric;
            this.childStore = childStore;
            this.mclazz = mclazz;
        }

        @Override
        protected Item createPutItem(HK hk, CK ck) {
            M m = scanKeyMetric(childStore, nodeMetric, ck);
            return super.createPutItem(hk, ck).with("m", m);
        }

        @Override
        protected void createTable() {
            LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex()
                .withIndexName("metric")
                .withKeySchema(new KeySchemaElement("hk", KeyType.HASH), new KeySchemaElement("m", KeyType.RANGE))
                .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));

            table = makeTable(makeTableRequest(tableName, "hk", keyType(hostKeyClass), "ck", keyType(childKeyClass)) // TODO copied from multilink
                .withLocalSecondaryIndexes(localSecondaryIndex)
                .withAttributeDefinitions(new AttributeDefinition("m", keyType(mclazz)))
            );
        }

        @Override
        public void update(HK hk, CK ck) {
            // TODO: better solution is possible
           delete(hk, ck);
           put(hk, ck);
        }

        @Override
        public List<CK> keys(HK hk, ISecQuery<CK, M> query) {

            System.err.println("DynamoDB SecMultiLink range query: " + tableName + "/" + hk);

            Index index = table.getIndex("metric");
            return scanSecondary(query, index, childStore, nodeMetric, keyFor(hk), childKeyClass, mclazz, "ck", "m", this::createTable);
        }

    }




}
