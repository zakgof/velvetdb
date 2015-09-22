package com.zakgof.db.kvs.impl;

/*

import java.io.ByteArrayInputStream;
import java.util.Map.Entry;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.serialize.ZeSerializer;
import com.zakgof.tools.Buffer;

public class BerkeleyKvs implements ITransactionalKvs {

  private Database berkeley;
  private Transaction txn;

  public BerkeleyKvs(Database berkeley, Transaction txn) {
    this.berkeley = berkeley;
    this.txn = txn;
  }
  
  @Override
  public <T> T get(Class<T> clazz, Object key) {
    ZeSerializer serializer = new ZeSerializer();
    byte[] keyBytes = serializer.serialize(key);
    DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
    DatabaseEntry valueEntry = new DatabaseEntry();
    try {
      OperationStatus status = berkeley.get(txn, keyEntry, valueEntry, LockMode.DEFAULT);
      if (status == OperationStatus.NOTFOUND)
        return null;
      byte[] data = valueEntry.getData();
      reads++;
      readBytes += data.length;
      T value = serializer.deserialize(new ByteArrayInputStream(data), clazz);
      return value;
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> void put(Object key, T value) {
    ZeSerializer serializer = new ZeSerializer();
    byte[] keyBytes = serializer.serialize(key);
    DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
    byte[] valueBytes = serializer.serialize(value);
    DatabaseEntry valueEntry = new DatabaseEntry(valueBytes);    
    try {
      berkeley.put(txn, keyEntry, valueEntry);
      writes++;
      writeBytes += keyBytes.length + valueBytes.length;
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(Object key) {
    ZeSerializer serializer = new ZeSerializer();
    byte[] keyBytes = serializer.serialize(key);
    DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
    try {
      berkeley.delete(txn, keyEntry);
      writes++;
      writeBytes += keyBytes.length;
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rollback() {
    try {
      txn.abort();
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commit() {
    try {
      txn.commit();
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void putRaw(Entry<Buffer, Buffer> entry) {
    byte[] keyBytes = entry.getKey().bytes();
    DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
    byte[] valueBytes = entry.getValue().bytes();
    DatabaseEntry valueEntry = new DatabaseEntry(valueBytes);
    try {
      berkeley.put(txn, keyEntry, valueEntry);
      writes++;
      writeBytes += keyBytes.length + valueBytes.length;
    } catch (DatabaseException e) {
      throw new RuntimeException(e);
    }

  }

  private long reads = 0;
  private long readBytes = 0;
  private long writes = 0;
  private long writeBytes = 0;

  public void resetStats() {

  }

  public String dumpStats() {
    return "Berkeley KVS stats: reads  " + reads + "\t" + readBytes + "\twrites " + writes + "\t" + writeBytes;
  }

}
*/