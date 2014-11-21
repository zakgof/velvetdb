package com.zakgof.db.sqlkvs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.serialize.ZeSerializer;
import com.zakgof.tools.Buffer;
import com.zakgof.tools.io.SimpleInputStream;
import com.zakgof.tools.io.SimpleOutputStream;

public class MemKvs implements ITransactionalKvs {
  
  private final Map<Buffer, Buffer> values = new HashMap<>();
  
  private long reads = 0;
  private long readBytes = 0;  
  private long writes = 0;
  private long writeBytes = 0;

  @Override
  public <T> T get(Class<T> clazz, Object key) {
    ZeSerializer serializer = new ZeSerializer();
    Buffer keyBuffer = new Buffer(serializer.serialize(key));
    Buffer valueBuffer = values.get(keyBuffer);
    if (valueBuffer == null)
      return null;
    T value = serializer.deserialize(valueBuffer.stream(), clazz);
    reads++;
    readBytes += keyBuffer.size() + valueBuffer.size();
    return value;
  }

  @Override
  public <T> void put(Object key, T value) {
    ZeSerializer serializer = new ZeSerializer();
    Buffer keyBuffer = new Buffer(serializer.serialize(key));
    Buffer valueBuffer = new Buffer(serializer.serialize(value));
    values.put(keyBuffer, valueBuffer);
    writes++;
    writeBytes += keyBuffer.size() + valueBuffer.size();
  }

  @Override
  public void delete(Object key) {
    ZeSerializer serializer = new ZeSerializer();
    Buffer keyBuffer = new Buffer(serializer.serialize(key));
    values.remove(keyBuffer);
    writes++;
    writeBytes += keyBuffer.size();
  }

  @Override
  public void rollback() {
    // Not implemented
  }

  @Override
  public void commit() {
    // TODO Auto-generated method stub
  }

  public void persist(OutputStream stream) throws IOException {
    SimpleOutputStream sos = new SimpleOutputStream(stream);
    sos.write(values.size());
    for (Entry<Buffer, Buffer> entry : values.entrySet()) {
      sos.write(entry.getKey().bytes());
      sos.write(entry.getValue().bytes());
    }    
    stream.flush();
    stream.close();
  }

  public void load(InputStream is) throws IOException {
    SimpleInputStream sis = new SimpleInputStream(is);
    int size = sis.readInt();
    for (int i=0; i<size; i++) {
      byte[] keyBytes = sis.readBytes();
      byte[] valueBytes = sis.readBytes();
      values.put(new Buffer(keyBytes), new Buffer(valueBytes));
    }
    sis.close();
  }

  public void dump() {
    System.err.println("reads  " + reads + "\t" + readBytes);
    System.err.println("writes " + writes + "\t" + writeBytes);        
  }

}
