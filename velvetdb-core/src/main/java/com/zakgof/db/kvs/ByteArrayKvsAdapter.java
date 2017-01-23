package com.zakgof.db.kvs;

import java.io.ByteArrayInputStream;

import com.zakgof.serialize.ZeSerializer;

public class ByteArrayKvsAdapter implements IKvs {

    private IByteArrayKvs bakvs;

    public ByteArrayKvsAdapter(IByteArrayKvs bakvs) {
        this.bakvs = bakvs;
    }

    @Override
    public <T> T get(Class<T> clazz, Object key) {
        ZeSerializer serializer = new ZeSerializer();
        byte[] keyBytes = serializer.serialize(key);

        byte[] valueBytes = bakvs.get(keyBytes);
        if (valueBytes == null)
            return null;

        reads++;
        readBytes += valueBytes.length;

        T value = serializer.deserialize(new ByteArrayInputStream(valueBytes), clazz);
        return value;
    }

    @Override
    public <T> void put(Object key, T value) {
        ZeSerializer serializer = new ZeSerializer();
        byte[] keyBytes = serializer.serialize(key);
        byte[] valueBytes = serializer.serialize(value);
        bakvs.put(keyBytes, valueBytes);
        writes++;
        writeBytes += keyBytes.length + valueBytes.length;
    }

    @Override
    public void delete(Object key) {
        ZeSerializer serializer = new ZeSerializer();
        byte[] keyBytes = serializer.serialize(key);
        bakvs.delete(keyBytes);
        writes++;
        writeBytes += keyBytes.length;
    }

    private long reads = 0;
    private long readBytes = 0;
    private long writes = 0;
    private long writeBytes = 0;

    public void resetStats() {

    }

    public String dumpStats() {
        return "ByteArrayKvs stats: reads  " + reads + "\t" + readBytes + "\twrites " + writes + "\t" + writeBytes;
    }

}
