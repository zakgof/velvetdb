package com.zakgof.db.velvet.links;

class IndexQuery<C> {
  C p1;
  boolean inclusive1;
  C p2;
  boolean inclusive2;
  int limit; // TODO
  
  public IndexQuery(C p1, boolean inclusive1, C p2, boolean inclusive2) {
    this.p1 = p1;
    this.inclusive1 = inclusive1;
    this.p2 = p2;
    this.inclusive2 = inclusive2;
  }

  static <C> IndexQuery<C> range(C p1, boolean inclusive1, C p2, boolean inclusive2) {
    return new IndexQuery<C>(p1, inclusive1, p2, inclusive2);
  }
  
  static <C> IndexQuery<C> greater(C p1) {
    return new IndexQuery<C>(p1, false, null, false);
  }
  
  static <C> IndexQuery<C> less(C p2) {
    return new IndexQuery<C>(null, false, p2, false);
  }
  
  static <C> IndexQuery<C> greaterOrEq(C p1) {
    return new IndexQuery<C>(p1, true, null, false);
  }
  
  static <C> IndexQuery<C> lessOrEq(C p2) {
    return new IndexQuery<C>(null, false, p2, true);
  }
  
}