package com.zakgof.db.kvs.mapdb;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.zakgof.db.kvs.ByteArrayKvsAdapter;
import com.zakgof.db.kvs.IKvs;
import com.zakgof.tools.Buffer;

public class MapDbKvsInitializer {

  private DB db;
  private ConcurrentNavigableMap<Buffer, Buffer> map;
  private MapDbByteArrayKvs bakvs;

  public MapDbKvsInitializer(File path) {
    db = DBMaker.fileDB(path). closeOnJvmShutdown().make();
    map = db.treeMap("kvs");
  }
  
  public DB getDb() {
    return db;
  }
  
  public IKvs createKvs() {
    bakvs = new MapDbByteArrayKvs(db, map);
    return new ByteArrayKvsAdapter(bakvs);
  }

  public void close() {
    System.err.println("Records in mapdb : " + map.size());
    db.commit();
    db.compact();
    db.close();
  }
  
  public MapDbByteArrayKvs getBakvs() {
    return bakvs;
  }

}
