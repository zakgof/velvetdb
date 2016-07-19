package com.zakgof.db.kvs.mapdb;

import java.util.Map;

import org.mapdb.DB;

import com.zakgof.db.kvs.IByteArrayKvs;
import com.zakgof.tools.Buffer;

public class MapDbByteArrayKvs implements IByteArrayKvs {

  private DB db;
  private Map<Buffer, Buffer> map;

  public MapDbByteArrayKvs(DB db, Map<Buffer, Buffer> map) {
    this.db = db;
    this.map = map;
  }
  
  public byte[] get(byte[] keyBytes) {
    Buffer buff = map.get(new Buffer(keyBytes));
    return buff == null ? null : buff.bytes(); 
  }

  public void put(byte[] keyBytes, byte[] valueBytes) {
    map.put(new Buffer(keyBytes), new Buffer(valueBytes));
  }

  public void delete(byte[] keyBytes) {
    map.remove(new Buffer(keyBytes));
  }


}
