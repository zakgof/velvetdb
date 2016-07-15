package com.zakgof.db.velvet.entity;

import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;

public class Indexes {

  public static <M extends Comparable<? super M>, V> IStoreIndexDef<M, V> create(String name, Function<V, M> metric) {
    return new IStoreIndexDef<M, V> () {
      @Override
      public String name() {
        return name;
      }
      @Override
      public Function<V, M> metric() {
        return metric;
      }
    };
  }
}
