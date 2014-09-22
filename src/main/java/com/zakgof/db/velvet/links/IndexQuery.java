package com.zakgof.db.velvet.links;

public class IndexQuery<C> {
  C p1;
  boolean inclusive1;
  C p2;
  boolean inclusive2;
  int limit = -1;
  int offset = 0;
  public boolean descending;
  
  public IndexQuery(C p1, boolean inclusive1, C p2, boolean inclusive2, int offset, int limit, boolean descending) {
    this.p1 = p1;
    this.inclusive1 = inclusive1;
    this.p2 = p2;
    this.inclusive2 = inclusive2;
    this.offset = offset;
    this.limit = limit;    
    this.descending = descending;
  }
  
  public static <C> Builder<C> builder() {
    return new Builder<C>();
  }
  
  public static class Builder<C> {
    
    private C p1;
    private boolean inclusive1;
    private C p2;
    private boolean inclusive2;
    private int limit = -1;
    private int offset;
    private boolean descending;
    
    public Builder<C> greater(C p1) {
      this.p1 = p1;
      this.inclusive1 = false;
      return this;
    }
    
    public Builder<C> greaterOrEq(C p1) {
      this.p1 = p1;
      this.inclusive1 = true;
      return this;
    }
    
    public Builder<C> less(C p2) {
      this.p2 = p2;
      this.inclusive2 = false;
      return this;
    }
    
    public Builder<C> lessOrEq(C p2) {
      this.p2 = p2;
      this.inclusive2 = true;
      return this;
    }
    
    public Builder<C> limit(int limit) {      
      this.limit = limit;
      return this;
    }
    
    public Builder<C> offset(int offset) {      
      this.offset = offset;
      return this;
    }
    
    public Builder<C> descending() {      
      this.descending = true;
      return this;
    }
    
    public IndexQuery<C> build() {
      return new IndexQuery<C>(p1, inclusive1, p2, inclusive2, offset, limit, descending);
    }
    
  }

  static <C> IndexQuery<C> range(C p1, boolean inclusive1, C p2, boolean inclusive2) {
    return new IndexQuery<C>(p1, inclusive1, p2, inclusive2, 0, -1, false);
  }
  
  static <C> IndexQuery<C> greater(C p1) {
    return new IndexQuery<C>(p1, false, null, false, 0, -1, false);
  }
  
  static <C> IndexQuery<C> less(C p2) {
    return new IndexQuery<C>(null, false, p2, false, 0, -1, true);
  }
  
  static <C> IndexQuery<C> greaterOrEq(C p1) {
    return new IndexQuery<C>(p1, true, null, false, 0, -1, false);
  }
  
  static <C> IndexQuery<C> lessOrEq(C p2) {
    return new IndexQuery<C>(null, false, p2, true, 0, -1, true);
  }
  
}