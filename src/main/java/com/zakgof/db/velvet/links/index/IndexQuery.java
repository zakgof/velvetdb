package com.zakgof.db.velvet.links.index;



public class IndexQuery<B, C> {
  
  public static class Level<B, C> {
    Level(C p, boolean inclusive) {
      this.p = p;
      this.inclusive = inclusive;
    }
    Level(B node) {
      this.node = node;
    }
    B node;
    C p;
    boolean inclusive;
  }
  
  Level<B, C> l1;
  Level<B, C> l2;
  
//  C p1;
//  boolean inclusive1;
//  C p2;
//  boolean inclusive2;
  int limit = -1;
  int offset = 0;
  public boolean descending;
  
  public IndexQuery(Level<B, C> l1, Level<B, C> l2, int offset, int limit, boolean descending) {
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
    
    public Builder<B, C> greaterO(B node1) {
      this.l1 = new Level<>(node1);      
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
    
    public Builder<B, C> lessO(B node2) {
      this.l2 = new Level<>(node2);      
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

  public static <B, C> IndexQuery<B, C> range(C p1, boolean inclusive1, C p2, boolean inclusive2) {
    return new IndexQuery<B, C>(new Level<>(p1, inclusive1), new Level<>(p2, inclusive2), 0, -1, false);
  }
  
  public static <B, C> IndexQuery<B, C> greater(C p1) {
    return IndexQuery.<B, C>builder().greater(p1).build();
  }
  
  public static <B, C> IndexQuery<B, C> less(C p2) {
    return IndexQuery.<B, C>builder().less(p2).build();
  }
  
  public static <B, C> IndexQuery<B, C> greaterOrEq(C p1) {
    return IndexQuery.<B, C>builder().greaterOrEq(p1).build();
  }
  
  public static <B, C> IndexQuery<B, C> lessOrEq(C p2) {
    return IndexQuery.<B, C>builder().lessOrEq(p2).build();
  }

  public static <B, C> IndexQuery<B, C> last() {
    return IndexQuery.<B, C>builder().descending().limit(1).build();
  }
  
  public static <B, C> IndexQuery<B, C> prev(B node) {
    return IndexQuery.<B, C>builder().lessO(node).descending().limit(1).build();
  }
  
  public static <B, C> IndexQuery<B, C> next(B node) {
    return IndexQuery.<B, C>builder().greaterO(node).limit(1).build();
  }
  
  public static <B, C> IndexQuery<B, C> equalsTo(C p) {
    return IndexQuery.<B, C>builder().greaterOrEq(p).lessOrEq(p).build();
  }
  
}