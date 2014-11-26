package com.zakgof.db.velvet;

public class IndexQuery<K, M> {
  
  public static class Level<K, M> {
    Level(M m, boolean inclusive) {
      this.m = m;
      this.inclusive = inclusive;
    }
    Level(K key) {
      this.key = key;
    }
    public K key;
    public M m;
    public boolean inclusive;
  }
  
  public Level<K, M> l1;
  public Level<K, M> l2;
  
//  C p1;
//  boolean inclusive1;
//  C p2;
//  boolean inclusive2;
  public int limit = -1;
  public int offset = 0;
  public boolean descending;
  
  public IndexQuery(Level<K, M> l1, Level<K, M> l2, int offset, int limit, boolean descending) {
    this.l1 = l1;
    this.l2 = l2;
    this.offset = offset;
    this.limit = limit;    
    this.descending = descending;
  }
  
  public static <B, C> Builder<B, C> builder() {
    return new Builder<B, C>();
  }
  
  public static class Builder<B, C> {
    
    private int limit = -1;
    private int offset;
    private boolean descending;
    private Level<B, C> l1;
    private Level<B, C> l2;
    
    public Builder<B, C> greater(C p1) {      
      this.l1 = new Level<>(p1, false);
      return this;
    }
    
    public Builder<B, C> greaterO(B key1) {
      this.l1 = new Level<>(key1);      
      return this;
    }
    
    public Builder<B, C> greaterOrEq(C p1) {
      this.l1 = new Level<>(p1, true);
      return this;
    }
    
    public Builder<B, C> less(C p2) {
      this.l2 = new Level<>(p2, false);
      return this;
    }
    
    public Builder<B, C> lessO(B key2) {
      this.l2 = new Level<>(key2);      
      return this;
    }
    
    public Builder<B, C> lessOrEq(C p2) {
      this.l2 = new Level<>(p2, true);
      return this;
    }
    
    public Builder<B, C> limit(int limit) {      
      this.limit = limit;
      return this;
    }
    
    public Builder<B, C> offset(int offset) {      
      this.offset = offset;
      return this;
    }
    
    public Builder<B, C> descending() {      
      this.descending = true;
      return this;
    }
    
    public IndexQuery<B, C> build() {
      return new IndexQuery<B, C>(l1, l2, offset, limit, descending);
    }
    
  }
  
}