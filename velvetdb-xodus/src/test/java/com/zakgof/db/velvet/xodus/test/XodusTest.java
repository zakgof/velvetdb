package com.zakgof.db.velvet.xodus.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;

public class XodusTest {
  
  public static void main(String[] args) throws InterruptedException {
    
    final Environment env = Environments.newInstance("D:\\Lab\\xodus2");
    env.executeInTransaction(txn -> {
      
      
      Store store = env.openStore("store", StoreConfig.WITH_DUPLICATES, txn);
      
      for (int i=0; i<90000; i++) {
        ArrayByteIterable key = StringBinding.stringToEntry("" + i);
        for (int j=0; j<5; j++) {
          ArrayByteIterable val = LongBinding.longToEntry(j);
          store.put(txn, key, val);
        }
      }
      
      Cursor cursor = store.openCursor(txn);
      cursor.getLast();
      
      ByteIterable key = cursor.getKey();
      ByteIterable val = cursor.getValue();
      String k = StringBinding.entryToString(key);
      long v = LongBinding.entryToLong(val);
      System.err.println(k + " " + v);
      
    });
    
    
  }

  public static void main2(String[] args) throws InterruptedException {

    
    final Environment env = Environments.newInstance("D:\\Lab\\xodus2");
    
    int THREADS = 10;
    int WRITES = 50000;
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    
    for (int t=0; t<THREADS; t++) {
      final int t1 = t;
      pool.execute(() -> {
        Transaction txn = env.beginTransaction();
        for(int a=0;;a++) {          
          Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);      
          for (int i=0; i<WRITES; i++) {
            store.put(txn, StringBinding.stringToEntry("key" + t1 + "." + i), StringBinding.stringToEntry("value" + t1 + "." + i));
          }
          if (txn.flush())
            break;  
          txn.revert();        
          System.err.println("Thread " + t1 + " Reattempt = " + a);
        }
        System.err.println("Thread " + t1 + " success");
        txn.abort();
      });
    }
    
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.DAYS);
    
    env.executeInTransaction(txn -> {
      Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
      for (int t=0; t<THREADS; t++)
        for (int i=0; i<WRITES; i++) {
          ByteIterable valbi = store.get(txn, StringBinding.stringToEntry("key" + t + "." + i));
          String value = StringBinding.entryToString(valbi);
          if (!value.equals("value" + t + "." + i))
            System.err.println("ERROR");
      }
    });
    
    env.close();
    
  }

  protected static void run(Environment env, Transaction txn) {
    Store store = env.openStore("store", StoreConfig.WITHOUT_DUPLICATES, txn);
    
    
    
    store.put(txn, StringBinding.stringToEntry("a-key"), StringBinding.stringToEntry("a-value"));
    store.put(txn, StringBinding.stringToEntry("b-key"), StringBinding.stringToEntry("b-value"));
    store.put(txn, StringBinding.stringToEntry("c-key"), StringBinding.stringToEntry("c-value"));
    store.put(txn, StringBinding.stringToEntry("c-key"), StringBinding.stringToEntry("c-value-2"));
    store.put(txn, StringBinding.stringToEntry("c-key"), StringBinding.stringToEntry("c-value-3"));
    
    
    Cursor cursor = store.openCursor(txn);
    while (cursor.getNext());
    cursor.getLast();
    System.err.println("AFTER " + StringBinding.entryToString(cursor.getKey()) + "     ->     " + StringBinding.entryToString(cursor.getValue()) + " ");
    cursor.getPrev();
    System.err.println("PREV " + StringBinding.entryToString(cursor.getKey()) + "     ->     " + StringBinding.entryToString(cursor.getValue()) + " ");
    cursor.getNext();
    System.err.println("PREV/NEXT " + StringBinding.entryToString(cursor.getKey()) + "     ->     " + StringBinding.entryToString(cursor.getValue()) + " ");
    
      
    
//    for (;;) {
//      if (!cursor.getNext())
//        break;
//      ByteIterable key = cursor.getKey();
//      ByteIterable value = cursor.getValue();
//      System.err.println(key + "     ->     " + value + " ");
//      // System.err.println(key + "   " + obj(key) + "     ->     " + value + " " + obj(value));
//    }\

    for (;;) {
      if (!cursor.getNext())
        break;
      ByteIterable key = cursor.getKey();
      ByteIterable value = cursor.getValue();
      System.err.println(key + "     ->     " + value + " ");
      // System.err.println(key + "   " + obj(key) + "     ->     " + value + " " + obj(value));
    }
    System.err.println("AFTER " + cursor.getKey() + "     ->     " + cursor.getValue() + " ");

    
    boolean x = cursor.getLast();
    
    System.err.println("LAST " + StringBinding.entryToString(cursor.getKey()) + "     ->     " + StringBinding.entryToString(cursor.getValue()) + " ");
    
    cursor.getPrev();
    
    System.err.println("LAST/PREV " + cursor.getKey() + "     ->     " + cursor.getValue() + " ");
    
    cursor.getNext();
    
    System.err.println("LAST/PREV/NEXT " + cursor.getKey() + "     ->     " + cursor.getValue() + " ");
    
    

//    System.err.println("Inserting 10");
//    store.put(txn, bi("key10"), bi("val1"));
//    
//    System.err.println("Inserting 20");
//    store.put(txn, bi("key20"), bi("val2"));
//    
//    System.err.println("Inserting 30");
//    store.put(txn, bi("key30"), bi("val3"));
//    
//    System.err.println("Inserting 40");
//    store.put(txn, bi("key40"), bi("val3"));
//    
//    System.err.println("Inserting 50");
//    store.put(txn, bi("key50"), bi("val3"));
    
    txn.abort();
//    store.put(txn, bi("key1"), bi("val0"));
//    store.put(txn, bi("key1"), bi("val4"));
//    store.put(txn, bi("key1"), bi("val5"));
    
//    ByteIterable searchKey = cursor.getSearchKey(bi("key1"));
//    System.err.println("SK: " + searchKey + " ");
//    if (searchKey != null) {
//      System.err.println("SK: " + obj(searchKey));
//    }
//
//    ByteIterable keySK = cursor.getKey();
//    ByteIterable valueSK = cursor.getValue();
//    System.err.println("SEARCHED " + keySK + "   " + (valueSK.getLength() == 0 ? "NULL" : obj(valueSK)));

    for (;;) {
      if (!cursor.getNext())
        break;
      ByteIterable key = cursor.getKey();
      ByteIterable value = cursor.getValue();
      System.err.println(key + "     ->     " + value + " ");
      // System.err.println(key + "   " + obj(key) + "     ->     " + value + " " + obj(value));
    }
    env.close();

  }

//  private static <T> ByteIterable bi(T obj) {
//    return new RevByteIterable(obj);
//  }
//
//  private static String obj(ByteIterable bi) {
//    ZeSerializer ze = new ZeSerializer();
//    byte[] bytes = bi.getBytesUnsafe();
//    int length = bi.getLength();
//    InputStream inputStream = new ByteArrayInputStream(bytes, 0, length);
//    return ze.deserialize(inputStream, String.class);
//  }

}
