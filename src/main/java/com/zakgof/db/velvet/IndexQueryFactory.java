package com.zakgof.db.velvet;

import com.zakgof.db.velvet.IndexQuery.Level;

public class IndexQueryFactory {
  
  public static <B, M> IndexQuery<B, M> range(M p1, boolean inclusive1, M p2, boolean inclusive2) {
    return new IndexQuery<B, M>(new Level<>(p1, inclusive1), new Level<>(p2, inclusive2), 0, -1, false);
  }
  
  public static <B, M> IndexQuery<B, M> greater(M p1) {
    return IndexQuery.<B, M>builder().greater(p1).build();
  }
  
  public static <B, M> IndexQuery<B, M> less(M p2) {
    return IndexQuery.<B, M>builder().less(p2).build();
  }
  
  public static <B, M> IndexQuery<B, M> greaterOrEq(M p1) {
    return IndexQuery.<B, M>builder().greaterOrEq(p1).build();
  }
  
  public static <B, M> IndexQuery<B, M> lessOrEq(M p2) {
    return IndexQuery.<B, M>builder().lessOrEq(p2).build();
  }

  public static <B, M> IndexQuery<B, M> last() {
    return IndexQuery.<B, M>builder().descending().limit(1).build();
  }
  
  public static <B, M> IndexQuery<B, M> equalsTo(M p) {
    return IndexQuery.<B, M>builder().greaterOrEq(p).lessOrEq(p).build();
  }
  
  public static <T, K, M> IndexQuery<K, M> prev(T node) {
    @SuppressWarnings("unchecked")
    K key = (K) VelvetUtil.keyOf(node);
    return IndexQuery.<K, M>builder().lessO(key).descending().limit(1).build();
  }
  
  public static <T, K, M> IndexQuery<K, M> next(T node) {
    @SuppressWarnings("unchecked")
    K key = (K) VelvetUtil.keyOf(node);
    return nextKey(key);
  }
  
  public static <T, K, M> IndexQuery<K, M> nextKey(K key) {
    return IndexQuery.<K, M>builder().greaterO(key).limit(1).build();
  }
  
  
}
