package com.zakgof.db.velvet.xodus;

import java.util.HashMap;
import java.util.Map;

import com.zakgof.db.velvet.VelvetException;

import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;

public class IKeyGen {

    private Map<String, Object> autoKeys = new HashMap<>();
    private Locker locker = new Locker();

    public <K> K acquire(String kind, Class<K> keyClass, Store store, Transaction txn) {
        return locker.with(kind, () -> {
            K lastKey = getLastKey(kind, keyClass, store, txn);
            K newKey = incrementKey(lastKey, keyClass);
            autoKeys.put(kind, newKey);
            return newKey;
        });
    }

    private <K> K getLastKey(String kind, Class<K> keyClass, Store store, Transaction txn) {
        Object lastGenKey = autoKeys.get(kind);
        if (lastGenKey != null)
            return keyClass.cast(lastGenKey);
        try (Cursor cursor = store.openCursor(txn)) {
            boolean b = cursor.getLast();
            cursor.getPrev();
            cursor.getNext(); // TODO: workaround for xodus bug
            if (b)
                return BytesUtil.keyBiToObj(keyClass, cursor.getKey());
        }
        return null;
    }

    private <K> K incrementKey(K lastKey, Class<K> keyClass) {
        if (keyClass == Long.class) {
            return keyClass.cast(lastKey == null ? 1L : (Long) lastKey + 1L);
        }
        throw new VelvetException("Unsupported autokey class: " + keyClass);
    }

}
