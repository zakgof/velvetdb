package com.zakgof.db.velvet.kvs;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.TxMaker;

import com.google.common.io.Files;

public class TxTest {

  public static void main(String[] args) {
    
    File subdir = Files.createTempDir();    
    TxMaker txMaker = DBMaker.fileDB(new File(subdir, "db")).makeTxMaker();
        
    DB tx1 = txMaker.makeTx();
    HTreeMap<Object, Object> map1 = tx1.hashMap("test");
    map1.put("key1", "value1");
    tx1.commit();
    
    DB tx2 = txMaker.makeTx();
    HTreeMap<Object, Object> map2 = tx2.hashMap("test");
    System.err.println(map2.get("key1"));
    map2.put("key1", "value2");
    
    DB tx3 = txMaker.makeTx();
    HTreeMap<Object, Object> map3 = tx3.hashMap("test");
    System.err.println(map3.get("key1"));
    map3.put("key1", "value3");
    
    tx3.commit();
    tx2.commit();
    
    DB tx4 = txMaker.makeTx();
    HTreeMap<Object, Object> map4 = tx4.hashMap("test");
    System.err.println(map4.get("key1"));
    
  }
}
