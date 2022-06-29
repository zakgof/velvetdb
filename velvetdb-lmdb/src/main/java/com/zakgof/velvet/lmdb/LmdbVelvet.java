package com.zakgof.velvet.lmdb;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.velvet.ISerializer;
import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.IIndexRequest;
import com.zakgof.velvet.entity.IEntityDef;
import org.lmdbjava.*;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LmdbVelvet implements IVelvet {

    private final Env<ByteBuffer> env;
    private final ISerializer serializer;

    LmdbVelvet(Env<ByteBuffer> env, ISerializer serializer) {
        this.env = env;
        this.serializer = serializer;
    }

    @Override
    public <K, V> void singlePut(IEntityDef<K, V> entityDef, V value) {
        Dbi<ByteBuffer> dbi = dbiForWrite(entityDef);
        txnWrite(txn -> put(dbi, entityDef, value, txn));
    }

    @Override
    public <K, V> V singleGet(IEntityDef<K, V> entityDef, K key) {
        Dbi<ByteBuffer> dbi = dbiForWrite(entityDef);
        return txnRead(txn -> get(dbi, key, entityDef, txn));
    }

    private <K, V> V get(Dbi<ByteBuffer> dbi, K key, IEntityDef<K, V> entityDef, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = serialize(entityDef.keyClass(), key); // TODO: ordered stuff ?
        ByteBuffer valueBuffer = dbi.get(txn, keyBuffer);
        return valueBuffer == null ? null : deserialize(valueBuffer, entityDef.valueClass());
    }

    private <T> T deserialize(ByteBuffer valueBuffer, Class<T> valueClass) {
        byte[] bytes = new byte[valueBuffer.remaining()];
        valueBuffer.get(bytes);
        return serializer.deserialize(new ByteArrayInputStream(bytes), valueClass);
    }

    @Override
    public <K, V> Map<K, V> multiGet(EntityDef<K, V> entityDef, Collection<K> keys) {
        Dbi<ByteBuffer> dbi = dbiForWrite(entityDef);
        return txnRead(txn ->
                keys.stream()
                        .flatMap(key -> Stream.of(get(dbi, key, entityDef, txn))
                                .filter(Objects::nonNull)
                                .map(value -> new AbstractMap.SimpleEntry<>(key, value))
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (u, v) -> u, LinkedHashMap::new))
        );
    }

    private <T> T txnRead(Function<Txn<ByteBuffer>, T> action) {
        T ret;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ret = action.apply(txn);
            txn.commit();
        } catch (Exception e) {
            throw new VelvetException(e);
        }
        return ret;
    }

    private void txnWrite(Consumer<Txn<ByteBuffer>> action) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            action.accept(txn);
            txn.commit();
        } catch (Exception e) {
            throw new VelvetException(e);
        }
    }

    @Override
    public <K, V> Map<K, V> multiGetAll(EntityDef<K, V> entityDef) {
        Dbi<ByteBuffer> dbi = dbiForWrite(entityDef);
        return txnRead(txn -> {
                    Map<K, V> map = new LinkedHashMap<K, V>();
                    try (CursorIterable<ByteBuffer> iterable = dbi.iterate(txn)) {
                        for (CursorIterable.KeyVal<ByteBuffer> kv : iterable) {
                            ByteBuffer valueBuffer = kv.val();
                            V value = deserialize(valueBuffer, entityDef.valueClass());
                            K key = entityDef.keyOf(value);
                            map.put(key, value);
                        }
                    }
                    return map;
                }
        );
    }

    @Override
    public <K, V, M> Map<K, V> multiIndexGet(IIndexRequest<K, V, M> indexRequest) {
        // TODO implement me
        return null;
    }

    @Override
    public <K, V> void singleDelete(IEntityDef<K, V> entityDef, K key) {

    }

    @Override
    public <K, V> void multiDelete(IEntityDef<K, V> entityDef, Collection<K> keys) {

    }

    @Override
    public <K, V> void initialize(IEntityDef<K, V> entityDef) {

    }

    @Override
    public <K, V> void multiPut(IEntityDef<K, V> entityDef, Collection<V> values) {
        Dbi<ByteBuffer> dbi = dbiForWrite(entityDef);
        txnWrite(txn -> {
            for (V value : values) {
                put(dbi, entityDef, value, txn);
            }
        });
    }

    private <V, K> Dbi<ByteBuffer> dbiForWrite(IEntityDef<K, V> entityDef) {
        return env.openDbi(entityDef.kind(), DbiFlags.MDB_CREATE);// TODO kind validation
    }

    private <K, V> void put(Dbi<ByteBuffer> dbi, IEntityDef<K, V> entityDef, V value, Txn<ByteBuffer> txn) {
        K key = entityDef.keyOf(value);
        ByteBuffer keyBuffer = serialize(entityDef.keyClass(), key); // TODO: ordered stuff ?
        ByteBuffer valueBuffer = serialize(entityDef.valueClass(), value);
        dbi.put(txn, keyBuffer, valueBuffer);
    }

    private <T> ByteBuffer serialize(Class<T> clazz, T object) {
        byte[] keyBytes = serializer.serialize(object, clazz);
        return ByteBuffer.allocateDirect(keyBytes.length).put(keyBytes).flip();
    }


}
