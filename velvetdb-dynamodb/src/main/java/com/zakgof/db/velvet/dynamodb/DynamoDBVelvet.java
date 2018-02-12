package com.zakgof.db.velvet.dynamodb;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.primitives.Primitives;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.query.IQueryAnchor;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.serialize.ISerializer;

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

    private <T> T calcOnTable(Callable<T> callable, Runnable tableCreator) {
        try {
            try {
                return callable.call();
            } catch (ResourceNotFoundException e) {
                System.err.println("Table not found: " + e);
                try {
                    tableCreator.run();
                    return callable.call();
                } catch (ResourceNotFoundException e2) {
                    System.err.println("ERROR : Table not found again: " + e2);
                    return callable.call();
                }
            }
        } catch (Exception e) {
            throw new VelvetException(e);
        }
    }

    private void doOnTable(Runnable runnable, Runnable tableCreator) {
        try {
            runnable.run();
        } catch (ResourceNotFoundException e) {
            System.err.println("Table not found: " + e);
            try {
                tableCreator.run();
                runnable.run();
            } catch (ResourceNotFoundException e2) {
                System.err.println("ERROR : Table not found again: " + e2);
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

    private Table makeTable(String kind, KeySchemaElement[] keySchemaElements, AttributeDefinition[] attributeDefinitions) {
        Table table = db.createTable(new CreateTableRequest()
           .withTableName(kind)
           .withKeySchema(keySchemaElements)
           .withAttributeDefinitions(attributeDefinitions)
           .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        );
        try {
            System.err.print("Creating table " + kind + "... ");
            table.waitForActive();
            System.err.println("done");
        } catch (InterruptedException e) {
            throw new VelvetException(e);
        }
        return table;
    }

    private Table makeTable(String kind, String hashKeyName, ScalarAttributeType hashKeyType) {
        return makeTable(kind,
            new KeySchemaElement[] {new KeySchemaElement(hashKeyName, KeyType.HASH)},
            new AttributeDefinition[] {new AttributeDefinition(hashKeyName, hashKeyType)}
        );
    }

    private Table makeTable(String kind, String hashKeyName, ScalarAttributeType hashKeyType, String rangeKeyName, ScalarAttributeType rangeKeyType) {
        return makeTable(kind,
            new KeySchemaElement[] {new KeySchemaElement(hashKeyName, KeyType.HASH), new KeySchemaElement(rangeKeyName, KeyType.RANGE)},
            new AttributeDefinition[] {new AttributeDefinition(hashKeyName, hashKeyType), new AttributeDefinition(rangeKeyName, rangeKeyType)}
        );
    }

    DynamoDBVelvet(DynamoDB db, Supplier<ISerializer> serializerSupplier) {
        this.db = db;
        this.serializerSupplier = serializerSupplier;
    }

    @Override
    public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new Store<>(kind, keyClass, valueClass, indexes);
    }

    @Override
    public <K extends Comparable<? super K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedStore<>(kind, keyClass, valueClass, indexes);
    }

    @Override
    public <HK, CK> ILink<HK, CK> simpleIndex(HK hostKey, Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind, LinkType type) {
        return type == LinkType.Single ? new SingleLink<>(hostKey, hostKeyClass, childKeyClass, edgekind) : new MultiLink<>(hostKey, hostKeyClass, childKeyClass, edgekind);
    }

    @Override
    public <HK, CK extends Comparable<? super CK>, T> IKeyIndexLink<HK, CK, CK> primaryKeyIndex(HK key1, Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <HK, CK, T, M extends Comparable<? super M>> IKeyIndexLink<HK, CK, M> secondaryKeyIndex(HK key1, Class<HK> hostKeyClass, String edgekind, Function<T, M> nodeMetric, Class<M> mclazz, Class<CK> keyClazz, IStore<CK, T> childStore) {
        // TODO Auto-generated method stub
        return null;
    }

    private class Store<K, V> implements IStore<K, V> {

        protected Class<K> keyClass;
        protected String kind;
        protected Class<V> valueClass;
        protected Table table;

        public Store(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.kind = kind;
            this.table = db.getTable(kind);
        }

        @Override
        public V get(K key) {
            Item item = calcOnTable(() -> getByKey(key), this::createTable);
            if (item == null) {
                return null;
            }
            V value = valueFromItem(item, valueClass, "v", false);
            return value;
        }

        @Override
        public byte[] getRaw(K key) {
            return null;
        }

        @Override
        public void put(K key, V value) {
            Object v = valueToObject(value);
            Item item = createPutItem(key, v);
            doOnTable(() -> table.putItem(item), this::createTable);
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
            ItemCollection<ScanOutcome> itemCollection = calcOnTable(() -> table.scan(new ScanSpec().withAttributesToGet("id")), this::createTable);
            List<K> keys = StreamSupport.stream(itemCollection.spliterator(), false).map(this::keyFromItem).collect(Collectors.toList());
            return keys;
        }

        @Override
        public boolean contains(K key) {
            Item item = table.getItem("id", key);
            return item != null;
        }

        @Override
        public long size() {
            // TODO
            return keys().size();
        }

        @Override
        public <M extends Comparable<? super M>> IStoreIndex<K, M> index(String name) {
            // TODO Auto-generated method stub
            return null;
        }



        protected Item getByKey(K key) {
            return table.getItem("id", key);
        }

        protected void createTable() {
            table = makeTable(kind, "id",  keyType(keyClass));
        }

        protected Item createPutItem(K key, Object v) {
            return new Item().with("v", v).withKeyComponents(keyFor(key));
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


    }

    private class SortedStore <K extends Comparable<? super K>, V> extends Store<K, V> implements ISortedStore<K, V> {

        public SortedStore(String kind, Class<K> keyClass, Class<V> valueClass, Collection<IStoreIndexDef<?, V>> indexes) {
            super(kind, keyClass, valueClass, indexes);
        }

        @Override
        public List<K> keys(IRangeQuery<K, K> query) {

            QuerySpec qs = new QuerySpec()
                .withAttributesToGet("id")
                .withHashKey("partition", 1);

            IQueryAnchor<K, K> lowAnchor = query.getLowAnchor();
            if (lowAnchor != null) {
                K k = lowAnchor.getKey() == null ? lowAnchor.getMetric() : lowAnchor.getKey();
                RangeKeyCondition condition = new RangeKeyCondition("id");
                condition = lowAnchor.isIncluding() ? condition.ge(k) : condition.gt(k);
                qs.withRangeKeyCondition(condition);
            }
            IQueryAnchor<K, K> highAnchor = query.getHighAnchor();
            if (highAnchor != null) {
                K k = highAnchor.getKey() == null ? highAnchor.getMetric() : highAnchor.getKey();
                RangeKeyCondition condition = new RangeKeyCondition("id");
                condition = highAnchor.isIncluding() ? condition.le(k) : condition.lt(k);
                qs.withRangeKeyCondition(condition);
            }
            if (query.getLimit() > 0) {
                int number = query.getLimit() + query.getOffset();
                qs.withMaxResultSize(number);
            }
            if (!query.isAscending()) {
                qs.withScanIndexForward(false);
            }

            List<K> keys = calcOnTable(() ->  {

                ItemCollection<QueryOutcome> itemCollection = table.query(qs);
                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .skip(query.getOffset())
                    .map(this::keyFromItem)
                    .collect(Collectors.toList());

            }, this::createTable);
            return keys;
        }

        @Override
        protected void createTable() {
            table = makeTable(kind, "partition", ScalarAttributeType.N, "id", keyType(keyClass));
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
        protected Item createPutItem(K key, Object v) {
            return new Item()
               .with("v", v)
               .withKeyComponent("partition", 1)
               .withKeyComponent("id", key);
        }

    }

    public void killAll() {
        TableCollection<ListTablesResult> listTables = db.listTables();
        for (Table table : listTables) {
            System.err.print("Deleting " + table.getTableName() + "... ");
            table.delete();
            try {
                table.waitForDelete();
                System.err.println("done!");
            } catch (InterruptedException e) {
                throw new VelvetException(e);
            }
        }
    }

    /**
     * Hash key: hk
     * Value: ck
     */
    private class SingleLink<HK, CK> implements ILink<HK, CK> {

        protected Table table;
        protected String tableName;
        protected HK hk;
        protected Class<HK> hostKeyClass;
        protected Class<CK> childKeyClass;

        public SingleLink(HK hostKey, Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            this.tableName = prefix() + edgekind;
            this.hk = hostKey;
            this.hostKeyClass = hostKeyClass;
            this.childKeyClass = childKeyClass;
            this.table = db.getTable(tableName);
        }

        protected String prefix() {
            return "__s-";
        }

        @Override
        public void put(CK ck) {
            Item item = createPutItem(ck);
            doOnTable(() -> table.putItem(item), this::createTable);
        }

        @Override
        public void delete(CK ck) {
            doOnTable(() -> table.deleteItem(keyFor()), this::createTable);
        }

        @Override
        public List<CK> keys() {
            Item item = calcOnTable(() -> table.getItem(new KeyAttribute("hk", hk)), this::createTable);
            if (item == null) {
                return Collections.emptyList();
            }
            CK ck = valueFromItem(item, childKeyClass, "ck", true);
            return Arrays.asList(ck);
        }

        @Override
        public boolean contains(CK ck) {
            List<CK> keys = keys();
            return !keys.isEmpty() && keys.get(0).equals(ck);
        }

        protected Item createPutItem(CK ck) {
            return new Item()
               .withKeyComponents(keyFor())
               .with("ck", ck);
        }

        protected void createTable() {
            table = makeTable(tableName, "hk", keyType(hostKeyClass));
        }

        protected KeyAttribute keyFor() {
            return new KeyAttribute("hk", hk);
        }

    }

    /**
     * Hash key: hk
     * Range key: ck
     */
    private class MultiLink<HK, CK> extends SingleLink<HK, CK> {

        public MultiLink(HK hostKey, Class<HK> hostKeyClass, Class<CK> childKeyClass, String edgekind) {
            super(hostKey, hostKeyClass, childKeyClass, edgekind);
        }

        @Override
        protected String prefix() {
            return "__m-";
        }

        @Override
        public void delete(CK ck) {
            doOnTable(() -> table.deleteItem(keyFor(ck)), this::createTable);
        }

        @Override
        public List<CK> keys() {
            List<CK> cks = calcOnTable(() -> {
                ItemCollection<QueryOutcome> itemCollection = table.query(keyFor());
                return StreamSupport.stream(itemCollection.spliterator(), false)
                    .map(item -> valueFromItem(item, childKeyClass, "ck", true))
                    .collect(Collectors.toList());
            }, this::createTable);
            return cks;
        }

        @Override
        protected Item createPutItem(CK ck) {
            return new Item()
               .withKeyComponents(keyFor(ck));
        }

        @Override
        protected void createTable() {
            table = makeTable(tableName, "hk", keyType(hostKeyClass), "ck", keyType(childKeyClass));
        }

        private KeyAttribute[] keyFor(CK ck) {
            return new KeyAttribute[] {new KeyAttribute("hk", hk), new KeyAttribute("ck", ck)};
        }

    }


}
